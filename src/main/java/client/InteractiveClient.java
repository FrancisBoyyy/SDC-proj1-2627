package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
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
import client.timers.OperationTimeout;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Interactive client, useful to test the system by hand. Reads commands from
 * the standard input:
 *
 * <pre>
 * balance &lt;account&gt;
 * history &lt;account&gt; [fromIndex]
 * transfer &lt;signer&gt; &lt;from&gt; &lt;to&gt; &lt;amount&gt;
 * accounts
 * quit
 * </pre>
 *
 * The replies of every replica are printed, as well as the result accepted by
 * the client (once {@code reply_quorum} replicas sent matching replies).
 */
public class InteractiveClient implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(InteractiveClient.class);

    /**
     * A line typed by the user. The line is read by another thread, and handed to
     * the protocol as an event (handlers of a protocol never run concurrently).
     */
    public record ConsoleCommand(String line) implements ProtocolEvent {
    }

    // A transfer typed by the user, waiting for the next sequence number of its
    // account (the client reads it from the replicas before sending the transfer)
    private record PendingTransfer(String signer, String from, String to, long amount) {
    }

    private final List<BabelNode> replicas;
    private final Map<BabelNode, String> replicaNames;
    private final Map<String, PrivateKey> clientKeys;
    private final Map<String, Set<String>> accountOwners;
    private final int replyQuorum;
    private final long operationTimeout;
    // Replies collected so far for each request
    private final Map<UUID, ReplyQuorum<BabelMessage>> pending;
    // Transfers waiting for the next sequence number of their account
    private final Map<UUID, PendingTransfer> waitingSeq;

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication (timers and
     * the commands typed by the user). The channel object is for inter machine
     * communication (with the replicas).
     */
    private Babel runtime;
    private BabelChannel channel;

    /**
     * @param replicas         the replicas of the system (requests are sent to all)
     * @param replicaNames     names of the replicas (node1, node2, ...), to print
     *                         their replies
     * @param clientKeys       private keys of all clients: the user can issue
     *                         transfers signed by any of them
     * @param accountOwners    the accounts of the system and their owners
     * @param replyQuorum      number of matching replies required to accept a
     *                         result (f+1 by default)
     * @param operationTimeout time (ms) to wait for the replies to a request
     */
    public InteractiveClient(List<BabelNode> replicas, Map<BabelNode, String> replicaNames,
            Map<String, PrivateKey> clientKeys, Map<String, Set<String>> accountOwners, int replyQuorum,
            long operationTimeout) {
        this.replicas = List.copyOf(replicas);
        this.replicaNames = Map.copyOf(replicaNames);
        this.clientKeys = clientKeys;
        this.accountOwners = new TreeMap<>(utils.AliasComparator.INSTANCE);
        this.accountOwners.putAll(accountOwners);
        this.replyQuorum = replyQuorum;
        this.operationTimeout = operationTimeout;
        this.pending = new HashMap<>();
        this.waitingSeq = new HashMap<>();
    }

    // This method is called by the Babel runtime itself. We connect to every
    // replica and start a thread that reads the console. That thread never touches
    // the state of the protocol: it hands each line to the protocol as an event.
    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
        for (var replica : replicas) {
            channel.connect(replica);
        }
        Thread reader = new Thread(() -> {
            try (var in = new BufferedReader(new InputStreamReader(System.in))) {
                String line;
                while ((line = in.readLine()) != null) {
                    runtime.deliverEvent(new ConsoleCommand(line));
                }
            } catch (IOException e) {
                LOGGER.error("Error reading the console", e);
            }
            runtime.deliverEvent(new ConsoleCommand("quit"));
        }, "console");
        reader.setDaemon(true);
        reader.start();
        System.out.println("Commands: balance <account> | history <account> [fromIndex] | "
                + "transfer <signer> <from> <to> <amount> | accounts | quit");
    }

    /**
     * Executes a command typed by the user. A transfer requires the next sequence
     * number of the account, so the client first reads the balance of the account
     * and only then sends the transfer (see uponBalanceReply).
     *
     * @param command
     */
    @UponBabelEvent
    public void uponConsoleCommand(ConsoleCommand command) {
        String[] parts = command.line().strip().split("\\s+");
        try {
            switch (parts[0]) {
                case "" -> {
                }
                case "balance" -> send(new BalanceRequest(UUID.randomUUID(), parts[1]));
                case "history" -> send(new HistoryRequest(UUID.randomUUID(), parts[1],
                        parts.length > 2 ? Integer.parseInt(parts[2]) : 0));
                case "transfer" -> {
                    var t = new PendingTransfer(parts[1], parts[2], parts[3], Long.parseLong(parts[4]));
                    if (!clientKeys.containsKey(t.signer())) {
                        System.out.println("Unknown signer " + t.signer());
                        return;
                    }
                    // First read the next sequence number of the account
                    UUID id = send(new BalanceRequest(UUID.randomUUID(), t.from()));
                    waitingSeq.put(id, t);
                }
                case "accounts" -> accountOwners.forEach((a, o) -> System.out.println(a + " owned by " + o));
                case "quit", "exit" -> System.exit(0);
                default -> System.out.println("Unknown command: " + parts[0]);
            }
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            System.out.println("Invalid command: " + command.line());
        }
    }

    /**
     * Prints the reply of a replica to a transfer, and the accepted result once
     * enough replicas sent matching replies (same status).
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponTransferReply(TransferReply reply, BabelNode replica) {
        print(replica, reply);
        accept(reply.requestId(), replica, reply.status() + ":" + reply.account() + ":" + reply.seq(), reply);
    }

    /**
     * Prints the reply of a replica to a balance request. If the balance was read
     * to learn the next sequence number of an account (to issue a transfer typed
     * by the user), the transfer is created, signed, and sent once the result is
     * accepted.
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponBalanceReply(BalanceReply reply, BabelNode replica) {
        print(replica, reply);
        BabelMessage accepted = accept(reply.requestId(), replica,
                reply.account() + ":" + reply.exists() + ":" + reply.balance() + ":" + reply.nextSeq(), reply);
        PendingTransfer t = accepted != null ? waitingSeq.remove(reply.requestId()) : null;
        PrivateKey key = t != null ? clientKeys.get(t.signer()) : null;
        if (t != null && key != null) {
            try {
                Transfer transfer = Transfer.create(t.from(), t.to(), t.amount(), reply.nextSeq(), t.signer(),
                        key);
                System.out.println("Sending " + transfer);
                send(new TransferRequest(UUID.randomUUID(), transfer));
            } catch (GeneralSecurityException e) {
                System.out.println("Could not sign the transfer: " + e);
            }
        }
    }

    /**
     * Prints the reply of a replica to a history request. Replies match if they
     * contain the same transfers, in the same order.
     *
     * @param reply
     * @param replica the replica that sent the reply (authenticated by the channel)
     */
    @UponMessageIn
    public void uponHistoryReply(HistoryReply reply, BabelNode replica) {
        print(replica, reply);
        StringBuilder key = new StringBuilder(reply.account()).append(':').append(reply.size());
        reply.transfers().forEach(t -> key.append(':').append(t.id()));
        accept(reply.requestId(), replica, key.toString(), reply);
    }

    /**
     * The replicas did not send enough matching replies in time (some replicas
     * might be down, or their replies might diverge).
     *
     * @param timeout
     */
    @UponBabelEvent
    public void uponOperationTimeout(OperationTimeout timeout) {
        var quorum = pending.remove(timeout.requestId());
        waitingSeq.remove(timeout.requestId());
        if (quorum != null) {
            System.out.println("Request " + timeout.requestId() + " timed out with " + quorum.replies()
                    + " replies (and no " + replyQuorum + " matching replies)");
        }
    }

    // Sends a request to all replicas, and starts its timeout
    private UUID send(BabelMessage request) {
        UUID id = switch (request) {
            case BalanceRequest r -> r.requestId();
            case HistoryRequest r -> r.requestId();
            case TransferRequest r -> r.requestId();
            default -> throw new IllegalArgumentException("Unexpected request " + request);
        };
        pending.put(id, new ReplyQuorum<>(replyQuorum));
        for (var replica : replicas) {
            channel.send(replica, request);
        }
        runtime.setUpTimer(new OperationTimeout(id), operationTimeout, TimeUnit.MILLISECONDS);
        return id;
    }

    // Registers the reply of a replica; returns the reply once it is accepted
    private @Nullable BabelMessage accept(UUID requestId, BabelNode replica, String key, BabelMessage reply) {
        var quorum = pending.get(requestId);
        if (quorum == null) {
            return null;
        }
        BabelMessage accepted = quorum.add(replica, key, reply);
        if (accepted != null) {
            pending.remove(requestId);
            System.out.println("ACCEPTED (" + replyQuorum + " matching replies): " + accepted);
        }
        return accepted;
    }

    private void print(BabelNode replica, BabelMessage reply) {
        System.out.println("  " + replicaNames.getOrDefault(replica, "unknown") + " -> " + reply);
    }
}
