package app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import app.ledger.Ledger;
import app.ledger.TransferOutcome;
import app.ledger.TransferStatus;
import app.messages.BalanceReply;
import app.messages.BalanceRequest;
import app.messages.HistoryReply;
import app.messages.HistoryRequest;
import app.messages.Transfer;
import app.messages.TransferReply;
import app.messages.TransferRequest;
import app.timers.StateDigestTimer;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

/**
 * Application layer of the replicated payment service (it is the same for all
 * the variants of the project).
 *
 * <p>
 * It has two responsibilities:
 * <ul>
 * <li>Interaction with clients: receives transfer, balance, and history
 * requests (through the client facing channel) and replies to them.</li>
 * <li>Management of the state: transfers received from clients are submitted
 * to the replication protocol ({@link SubmitTransaction}) and handed to the
 * {@link Ledger} only when the replication protocol says so
 * ({@link ExecuteTransaction}).</li>
 * </ul>
 *
 * <p>
 * A client is only answered when the outcome of its transfer is known. Read
 * operations (balance and history) are answered immediately with the local
 * state of this replica.
 */
public class PaymentService implements BabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(PaymentService.class);
    public static final int MAX_HISTORY_ENTRIES_PER_REPLY = 100;

    // A client waiting for the outcome of a transfer
    private record Waiter(BabelNode client, UUID requestId, String transferId) {
    }

    private final Ledger ledger;
    private final long digestInterval;

    // Clients waiting for the outcome of transfers, indexed by account#seq
    private final Map<String, List<Waiter>> waiting;
    // Transfers submitted by this replica that were not handed to the ledger yet
    private final Set<String> inFlight;
    // Position in the ledger of each executed transfer
    private final Map<String, Long> executedIndex;

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication. The channel
     * object is for inter machine communication (in this case, with clients).
     */
    private Babel runtime;
    private BabelChannel channel;

    public PaymentService(Ledger ledger, long digestInterval) {
        this.ledger = ledger;
        this.digestInterval = digestInterval;
        this.waiting = new HashMap<>();
        this.inFlight = new HashSet<>();
        this.executedIndex = new HashMap<>();
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
        LOGGER.info("Payment service ready with {} accounts (total supply {})", ledger.accounts().size(),
                ledger.totalSupply());
        if (digestInterval > 0) {
            runtime.setUpTimer(new StateDigestTimer(), digestInterval, digestInterval, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * A client asks for a transfer to be executed. Invalid transfers, and
     * transfers whose outcome is already known, are answered immediately.
     * Otherwise, the client waits for the outcome, and the transfer is submitted
     * to the replication protocol (unless it was already submitted).
     *
     * Remember: an exception that escapes a handler stops the protocol (Babel 2
     * stops processing its events), and clients might be Byzantine. Hence the
     * try-catch.
     */
    @UponMessageIn
    public void uponTransferRequest(TransferRequest request, BabelNode client) {
        try {
            Transfer t = request.transfer();
            TransferStatus status = ledger.statusOf(t);
            switch (status) {
                case INVALID, CONFLICT -> reply(client, request.requestId(), t, status, -1);
                case EXECUTED -> reply(client, request.requestId(), t, status, executedIndex.getOrDefault(t.id(), -1L));
                case PENDING, DUPLICATE -> {
                    waiting.computeIfAbsent(key(t), k -> new ArrayList<>())
                            .add(new Waiter(client, request.requestId(), t.id()));
                    if (status == TransferStatus.PENDING && inFlight.add(t.id())) {
                        LOGGER.debug("Submitting {} to the replication protocol", t);
                        runtime.deliverEvent(new SubmitTransaction(t));
                    }
                }
            }
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed transfer request from {}: {}", client, e.toString());
        }
    }

    /**
     * The replication protocol tells us to hand a transfer to the ledger. Every
     * transfer that is executed as a consequence is reported to the clients
     * waiting for it.
     */
    @UponBabelEvent
    public void uponExecuteTransaction(ExecuteTransaction execute) {
        try {
            Transfer t = execute.transfer();
            inFlight.remove(t.id());
            for (TransferOutcome outcome : ledger.execute(t)) {
                switch (outcome.status()) {
                    case EXECUTED -> {
                        executedIndex.put(outcome.transfer().id(), outcome.ledgerIndex());
                        LOGGER.debug("[[ {} ]] {}", outcome.ledgerIndex(), outcome.transfer());
                        notifyWaiters(outcome.transfer(), outcome.ledgerIndex());
                    }
                    case INVALID, CONFLICT -> notifyRejected(outcome.transfer(), outcome.status());
                    case PENDING, DUPLICATE -> {
                        // Nothing to report (yet)
                    }
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error("Could not execute {}", execute.transfer(), e);
        }
    }

    @UponMessageIn
    public void uponBalanceRequest(BalanceRequest request, BabelNode client) {
        try {
            String account = request.account();
            BalanceReply reply = ledger.exists(account)
                    ? new BalanceReply(request.requestId(), account, true, ledger.balance(account),
                            ledger.nextSeq(account))
                    : new BalanceReply(request.requestId(), account, false, 0, 0);
            channel.send(client, reply);
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed balance request from {}: {}", client, e.toString());
        }
    }

    @UponMessageIn
    public void uponHistoryRequest(HistoryRequest request, BabelNode client) {
        try {
            String account = request.account();
            HistoryReply reply = ledger.exists(account)
                    ? new HistoryReply(request.requestId(), account, request.fromIndex(),
                            ledger.historySize(account),
                            ledger.history(account, request.fromIndex(), MAX_HISTORY_ENTRIES_PER_REPLY))
                    : new HistoryReply(request.requestId(), account, request.fromIndex(), 0, List.of());
            channel.send(client, reply);
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed history request from {}: {}", client, e.toString());
        }
    }

    /**
     * Periodically log a digest of the state, so that the state of different
     * replicas can be compared (correct replicas should converge to the same
     * digest once all transfers were executed).
     */
    @UponBabelEvent
    public void uponStateDigestTimer(StateDigestTimer timer) {
        LOGGER.info("STATE executed={} pending={} supply_ok={} digest={}", ledger.size(),
                ledger.pendingTransfers(), ledger.sumOfBalances() == ledger.totalSupply(), ledger.stateDigest());
    }

    /**
     * A client disconnected: forget the replies it was waiting for (they can no
     * longer be delivered). The transfers themselves are not affected.
     */
    @Override
    public void connectionDown(BabelNode client, @Nullable Throwable cause) {
        LOGGER.debug("Client {} disconnected", client);
        for (Iterator<List<Waiter>> it = waiting.values().iterator(); it.hasNext();) {
            List<Waiter> waiters = it.next();
            waiters.removeIf(w -> w.client().equals(client));
            if (waiters.isEmpty()) {
                it.remove();
            }
        }
    }

    private void notifyWaiters(Transfer executed, long ledgerIndex) {
        List<Waiter> waiters = waiting.remove(key(executed));
        if (waiters == null) {
            return;
        }
        String id = executed.id();
        for (Waiter w : waiters) {
            // Waiters for a different transfer with the same account and seq lost
            TransferStatus status = w.transferId().equals(id) ? TransferStatus.EXECUTED : TransferStatus.CONFLICT;
            reply(w.client(), w.requestId(), executed, status, status == TransferStatus.EXECUTED ? ledgerIndex : -1);
        }
    }

    private void notifyRejected(Transfer rejected, TransferStatus status) {
        List<Waiter> waiters = waiting.get(key(rejected));
        if (waiters == null) {
            return;
        }
        String id = rejected.id();
        for (Iterator<Waiter> it = waiters.iterator(); it.hasNext();) {
            Waiter w = it.next();
            if (w.transferId().equals(id)) {
                reply(w.client(), w.requestId(), rejected, status, -1);
                it.remove();
            }
        }
        if (waiters.isEmpty()) {
            waiting.remove(key(rejected));
        }
    }

    private void reply(BabelNode client, UUID requestId, Transfer t, TransferStatus status, long ledgerIndex) {
        channel.send(client, new TransferReply(requestId, t.from(), t.seq(), status.name(), ledgerIndex));
    }

    private static String key(Transfer t) {
        return t.from() + "#" + t.seq();
    }
}
