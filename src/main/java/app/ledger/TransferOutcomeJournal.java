package app.ledger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import app.messages.Transfer;
import utils.Crypto;

/**
 * Append-only record of transfer observations made by one replica.
 *
 * This class does not change ledger or protocol behavior. It is created by
 * {@code Main} only when transfer auditing is enabled for an experiment.
 */
public final class TransferOutcomeJournal {

    private final List<TransferAuditEvent> events;
    private long nextOrdinal;

    public TransferOutcomeJournal() {
        this.events = new ArrayList<>();
        this.nextOrdinal = 0;
    }

    public TransferAuditEvent recordClientRequest(Transfer transfer) {
        return append(
                TransferAuditEvent.Stage.CLIENT_REQUEST,
                transfer,
                Optional.empty(),
                -1
        );
    }

    public TransferAuditEvent recordReplicationSubmission(Transfer transfer) {
        return append(
                TransferAuditEvent.Stage.REPLICATION_SUBMISSION,
                transfer,
                Optional.empty(),
                -1
        );
    }

    public TransferAuditEvent recordRequestStatus(
            Transfer transfer,
            TransferStatus status,
            long ledgerIndex
    ) {
        return append(
                TransferAuditEvent.Stage.REQUEST_STATUS,
                transfer,
                Optional.of(status),
                ledgerIndex
        );
    }

    public TransferAuditEvent recordLedgerOutcome(TransferOutcome outcome) {
        return append(
                TransferAuditEvent.Stage.LEDGER_OUTCOME,
                outcome.transfer(),
                Optional.of(outcome.status()),
                outcome.ledgerIndex()
        );
    }
    /**
     * @return an immutable copy of events in observation order
     */
    public List<TransferAuditEvent> snapshot() {
        return List.copyOf(events);
    }

    private TransferAuditEvent append(
            TransferAuditEvent.Stage stage,
            Transfer transfer,
            Optional<TransferStatus> status,
            long ledgerIndex
    ) {
        TransferAuditEvent event = new TransferAuditEvent(
                nextOrdinal++,
                stage,
                transfer.id(),
                Crypto.toHex(Crypto.digest(transfer.signature())),
                transfer.from(),
                transfer.to(),
                transfer.amount(),
                transfer.seq(),
                transfer.signer(),
                status,
                ledgerIndex
        );

        events.add(event);
        return event;
    }
}
