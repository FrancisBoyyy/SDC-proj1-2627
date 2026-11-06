package client;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import app.messages.BalanceReply;
import app.messages.BalanceRequest;
import app.messages.HistoryReply;
import app.messages.HistoryRequest;
import app.messages.Transfer;
import app.messages.TransferReply;
import app.messages.TransferRequest;
import client.metrics.Measurements;
import client.timers.ExitTimer;
import client.timers.OperationTimeout;
import client.timers.StartTimer;
import client.timers.StopTimer;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;

/**
 * Automated client used to generate load and to measure the performance of the
 * system (throughput and latency).
 *
 * <p>
 * A single process emulates several (logical) clients. Each client owns an
 * account (with the same name as the client) and may also be one of the owners
 * of shared accounts. Each client executes one operation at a time (closed
 * loop): a transfer (from its own account or from a shared account it owns) or
 * the read of a balance or of the history of an account.
 *
 * <p>
 * Every request is sent to all replicas, and the client waits for
 * {@code reply_quorum} matching replies from different replicas (f+1 by
 * default). A transfer from a shared account requires reading the next
 * sequence number of that account first; since other owners might do the same
 * concurrently, transfers from shared accounts may conflict.
 */
public class AutomatedClient implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(AutomatedClient.class);

    // Names of the types of operations, as they appear in the results
    public static final String TRANSFER = "transfer";
    public static final String SHARED_TRANSFER = "shared_transfer";
    public static final String BALANCE = "balance";
    public static final String HISTORY = "history";
    // Reads of the sequence number of the account of a client (e.g., after a timeout)
    public static final String SEQ_SYNC = "seq_sync";

    // Why the client issued an operation: to be measured (MEASURE), or to learn the
    // next sequence number of its own account (SYNC_OWN) or of a shared account
    // before issuing a transfer from it (SYNC_SHARED)
    private enum Purpose {
        MEASURE, SYNC_OWN, SYNC_SHARED
    }

    /**
     * A (logical) client emulated by this process. It owns the account with its
     * own name, and keeps track of the next sequence number of that account
     * (synced is false when the client does not know it, e.g., after a timeout).
     */
    private final class LogicalClient {
        private final String name;
        private final PrivateKey key;
        private final List<String> sharedAccounts;
        private long nextSeq;
        private boolean synced;

        private LogicalClient(String name, PrivateKey key, List<String> sharedAccounts) {
            this.name = name;
            this.key = key;
            this.sharedAccounts = sharedAccounts;
        }
    }

    /**
     * An operation waiting for the replies of the replicas. The timeout is
     * cancelled as soon as enough matching replies are received.
     */
    private record PendingOperation(LogicalClient client, String operation, Purpose purpose, long startNanos,
            BabelMessage request, ReplyQuorum<BabelMessage> quorum, ScheduledFuture<?> timeout) {
    }

    private final List<BabelNode> replicas;
    private final List<LogicalClient> clients;
    private final List<String> allAccounts;
    private final ClientParameters params;
    private final int replyQuorum;
    private final Random random;
    private final Measurements measurements;
    private final Map<UUID, PendingOperation> pending;
    private long divergentReplies;
    private boolean running;

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication (timers).
     * The channel object is for inter machine communication (with the replicas).
     */
    private Babel runtime;
    private BabelChannel channel;

    /**
     * @param replicas      the replicas of the system (requests are sent to all)
     * @param clientKeys    the private keys of all clients (from the clients key
     *                      store); this process only uses the keys of the clients
     *                      it emulates
     * @param accountOwners the accounts of the system and their owners
     * @param params        the parameters of the experiment
     * @param replyQuorum   the number of matching replies required to accept a
     *                      result (f+1 by default)
     */
    public AutomatedClient(List<BabelNode> replicas, Map<String, PrivateKey> clientKeys,
            Map<String, Set<String>> accountOwners, ClientParameters params, int replyQuorum) {
        this.replicas = List.copyOf(replicas);
        this.params = params;
        this.replyQuorum = replyQuorum;
        this.random = new Random();
        this.measurements = new Measurements();
        this.pending = new HashMap<>();
        this.allAccounts = new ArrayList<>(accountOwners.keySet());
        this.allAccounts.sort(utils.AliasComparator.INSTANCE);
        this.clients = new ArrayList<>(params.numClients());
        for (int i = params.firstClient(); i < params.firstClient() + params.numClients(); i++) {
            String name = "client" + i;
            PrivateKey key = clientKeys.get(name);
            if (key == null) {
                throw new IllegalArgumentException("There is no key for " + name + " in the clients key store");
            }
            // Shared accounts this client is one of the owners of
            List<String> shared = new ArrayList<>();
            for (var entry : accountOwners.entrySet()) {
                if (!entry.getKey().equals(name) && entry.getValue().contains(name)) {
                    shared.add(entry.getKey());
                }
            }
            clients.add(new LogicalClient(name, key, shared));
        }
    }

    // This method is called by the Babel runtime itself. We connect to every
    // replica and wait prepare_time before starting the experiment (so that the
    // connections are established, and replicas are ready).
    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
        for (var replica : replicas) {
            channel.connect(replica);
        }
        LOGGER.info("{} clients, {} replicas, waiting for {} matching replies. Starting in {} ms", clients.size(),
                replicas.size(), replyQuorum, params.prepareTime());
        runtime.setUpTimer(new StartTimer(), params.prepareTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * Starts the experiment: every client issues its first operation (each client
     * only issues a new operation when the previous one completes, i.e., in closed
     * loop). The experiment stops after {@code run_time}.
     *
     * @param timer
     */
    @UponBabelEvent
    public void uponStartTimer(StartTimer timer) {
        LOGGER.info("Starting operations... will stop in {} ms", params.runTime());
        running = true;
        measurements.start();
        runtime.setUpTimer(new StopTimer(), params.runTime(), TimeUnit.MILLISECONDS);
        for (var client : clients) {
            nextOperation(client);
        }
    }

    /**
     * Stops issuing operations (and measuring). The process terminates after
     * {@code cooldown_time}, allowing pending operations to finish.
     *
     * @param timer
     */
    @UponBabelEvent
    public void uponStopTimer(StopTimer timer) {
        running = false;
        measurements.stop();
        LOGGER.info("Stopped issuing operations... will terminate in {} ms", params.cooldownTime());
        runtime.setUpTimer(new ExitTimer(), params.cooldownTime(), TimeUnit.MILLISECONDS);
    }

    /**
     * Reports the results (in the log, in lines starting with RESULT, and in the
     * results file) and exits.
     *
     * @param timer
     */
    @UponBabelEvent
    public void uponExitTimer(ExitTimer timer) {
        for (var summary : measurements.summaries().values()) {
            LOGGER.info("RESULT {}", summary);
        }
        // Lower bound: replies that arrive after a result was accepted are not inspected
        LOGGER.info("RESULT operations with divergent replies (before acceptance): {}", divergentReplies);
        try {
            measurements.writeCsv(Path.of(params.resultsFile()), params.label());
            LOGGER.info("Results written to {}", params.resultsFile());
        } catch (IOException e) {
            LOGGER.error("Could not write the results file", e);
        }
        System.exit(0);
    }

    /**
     * The client did not receive enough matching replies in time. The operation is
     * counted as a timeout and the client moves on to its next operation.
     *
     * @param timeout
     */
    @UponBabelEvent
    public void uponOperationTimeout(OperationTimeout timeout) {
        PendingOperation op = pending.remove(timeout.requestId());
        if (op == null) {
            return;
        }
        LOGGER.debug("Operation {} of {} timed out ({} replies)", op.operation(), op.client().name,
                op.quorum().replies());
        switch (op.purpose()) {
            // A shared transfer that timed out while reading the sequence number of the
            // shared account is also a timed out shared transfer
            case MEASURE, SYNC_SHARED -> measurements.timeout(op.operation());
            case SYNC_OWN -> measurements.timeout(SEQ_SYNC);
        }
        if (op.request() instanceof TransferRequest request && request.transfer().from().equals(op.client().name)) {
            // We do not know whether the transfer will be executed: read the sequence
            // number of our account again before the next transfer
            op.client().synced = false;
        }
        nextOperation(op.client());
    }

    /**
     * Reply of a replica to a transfer. Replies match if they report the same
     * status for the same account and sequence number (the position in the ledger
     * is not compared, as it might be different at each replica when there is no
     * total order). The client only trusts what was compared: it uses the account
     * and sequence number of its own request, never those of a reply. When a
     * transfer from its own account is executed, the client advances its sequence
     * number; otherwise it reads it again before the next transfer.
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponTransferReply(TransferReply reply, BabelNode replica) {
        PendingOperation op = vote(reply.requestId(), replica,
                reply.status() + ":" + reply.account() + ":" + reply.seq(), reply);
        if (op == null) {
            return;
        }
        LogicalClient client = op.client();
        Transfer sent = ((TransferRequest) op.request()).transfer();
        boolean executed = "EXECUTED".equals(reply.status()) && reply.account().equals(sent.from())
                && reply.seq() == sent.seq();
        if (executed) {
            measurements.success(op.operation(), op.startNanos(), System.nanoTime());
        } else {
            measurements.failure(op.operation());
        }
        if (sent.from().equals(client.name)) {
            if (executed) {
                client.nextSeq = sent.seq() + 1;
            } else {
                client.synced = false;
            }
        }
        nextOperation(client);
    }

    /**
     * Reply of a replica to a balance request. Replies match if they report the
     * same account, balance and next sequence number; when the balance is only read
     * to learn the next sequence number of an account, only the sequence number has
     * to match (balances might differ while credits are being executed). Depending
     * on why the balance was read, the result is measured, used to learn the
     * sequence number of the account of the client, or used to issue a transfer
     * from a shared account.
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponBalanceReply(BalanceReply reply, BabelNode replica) {
        PendingOperation pendingOp = pending.get(reply.requestId());
        boolean onlySeq = pendingOp != null && pendingOp.purpose() != Purpose.MEASURE;
        String key = reply.account() + ":" + reply.exists() + ":" + reply.nextSeq()
                + (onlySeq ? "" : ":" + reply.balance());
        PendingOperation op = vote(reply.requestId(), replica, key, reply);
        if (op == null) {
            return;
        }
        LogicalClient client = op.client();
        String account = ((BalanceRequest) op.request()).account();
        if (!reply.account().equals(account) || !reply.exists()) {
            // Not the account we asked for (or it does not exist): read it again later
            if (op.purpose() == Purpose.MEASURE) {
                measurements.failure(op.operation());
            }
            nextOperation(client);
            return;
        }
        switch (op.purpose()) {
            case MEASURE -> {
                measurements.success(op.operation(), op.startNanos(), System.nanoTime());
                nextOperation(client);
            }
            case SYNC_OWN -> {
                client.nextSeq = reply.nextSeq();
                client.synced = true;
                nextOperation(client);
            }
            case SYNC_SHARED -> sendTransfer(client, account, reply.nextSeq(), SHARED_TRANSFER,
                    op.startNanos());
        }
    }

    /**
     * Reply of a replica to a history request. Replies match if they report the
     * same transfers, in the same order.
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponHistoryReply(HistoryReply reply, BabelNode replica) {
        StringBuilder key = new StringBuilder(reply.account()).append(':').append(reply.size());
        for (Transfer t : reply.transfers()) {
            key.append(':').append(t.from()).append('#').append(t.seq());
        }
        PendingOperation op = vote(reply.requestId(), replica, key.toString(), reply);
        if (op == null) {
            return;
        }
        measurements.success(op.operation(), op.startNanos(), System.nanoTime());
        nextOperation(op.client());
    }

    /**
     * Registers the reply of a replica. Returns the pending operation if this
     * reply completed a quorum of matching replies.
     */
    private @Nullable PendingOperation vote(UUID requestId, BabelNode replica, String matchKey,
            BabelMessage reply) {
        PendingOperation op = pending.get(requestId);
        if (op == null || !replicas.contains(replica)) {
            return null;
        }
        boolean wasDivergent = op.quorum().divergent();
        BabelMessage accepted = op.quorum().add(replica, matchKey, reply);
        if (!wasDivergent && op.quorum().divergent()) {
            divergentReplies++;
            LOGGER.debug("Divergent replies for {} of {}", op.operation(), op.client().name);
        }
        if (accepted == null) {
            return null;
        }
        pending.remove(requestId);
        op.timeout().cancel(false);
        return op;
    }

    /**
     * Chooses (randomly, according to the configured percentages) and issues the
     * next operation of a client. A client that does not know the next sequence
     * number of its account reads it first.
     */
    private void nextOperation(LogicalClient client) {
        if (!running) {
            return;
        }
        if (!client.synced) {
            sendBalance(client, client.name, Purpose.SYNC_OWN, BALANCE);
            return;
        }
        int r = random.nextInt(100);
        if (r < params.transferPercentage()) {
            if (!client.sharedAccounts.isEmpty() && random.nextInt(100) < params.sharedPercentage()) {
                String shared = client.sharedAccounts.get(random.nextInt(client.sharedAccounts.size()));
                // First read the next sequence number of the shared account
                sendBalance(client, shared, Purpose.SYNC_SHARED, SHARED_TRANSFER);
            } else {
                sendTransfer(client, client.name, client.nextSeq, TRANSFER, System.nanoTime());
            }
        } else if (r < params.transferPercentage() + params.historyPercentage()) {
            String account = randomAccount(null);
            UUID id = UUID.randomUUID();
            send(new PendingOperation(client, HISTORY, Purpose.MEASURE, System.nanoTime(),
                    new HistoryRequest(id, account, 0), new ReplyQuorum<>(replyQuorum), timeout(id)), id);
        } else {
            sendBalance(client, randomAccount(null), Purpose.MEASURE, BALANCE);
        }
    }

    // Reads the balance (and next sequence number) of an account
    private void sendBalance(LogicalClient client, String account, Purpose purpose, String operation) {
        UUID id = UUID.randomUUID();
        send(new PendingOperation(client, operation, purpose, System.nanoTime(), new BalanceRequest(id, account),
                new ReplyQuorum<>(replyQuorum), timeout(id)), id);
    }

    // Creates a transfer from account "from" to a random account, signed with the
    // key of the client (which must be one of the owners of "from")
    private void sendTransfer(LogicalClient client, String from, long seq, String operation, long startNanos) {
        try {
            long amount = 1 + random.nextInt(params.maxAmount());
            Transfer t = Transfer.create(from, randomAccount(from), amount, seq, client.name, client.key);
            UUID id = UUID.randomUUID();
            send(new PendingOperation(client, operation, Purpose.MEASURE, startNanos, new TransferRequest(id, t),
                    new ReplyQuorum<>(replyQuorum), timeout(id)), id);
        } catch (GeneralSecurityException e) {
            LOGGER.error("Could not sign transfer", e);
        }
    }

    // Every request is sent to all replicas (a single replica might be Byzantine)
    private void send(PendingOperation op, UUID id) {
        pending.put(id, op);
        for (var replica : replicas) {
            channel.send(replica, op.request());
        }
    }

    // Timer that fires if the operation does not complete in time
    private ScheduledFuture<?> timeout(UUID id) {
        return runtime.setUpTimer(new OperationTimeout(id), params.operationTimeout(), TimeUnit.MILLISECONDS);
    }

    private String randomAccount(@Nullable String excluded) {
        String account;
        do {
            account = allAccounts.get(random.nextInt(allAccounts.size()));
        } while (account.equals(excluded));
        return account;
    }
}
