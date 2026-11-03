package app.ledger;

import java.util.Objects;
import java.util.Optional;

/**
 * One observation of a transfer by a replica.
 *
 * @param ordinal      order in which this replica recorded the event
 * @param stage        where in the request and execution path it was observed
 * @param transferId   operation identifier, derived from signed content
 * @param signatureDigest digest of the signature bytes, distinguishing
 *                        different signatures for the same transfer ID
 * @param from         source account
 * @param to           destination account
 * @param amount       transfer amount
 * @param accountSeq   sequence number in the source account
 * @param signer       claimed signer
 * @param status       present for status and ledger-outcome events
 * @param ledgerIndex  execution index, or -1 if this event has no index
 */
public record TransferAuditEvent(
        long ordinal,
        Stage stage,
        String transferId,
        String signatureDigest,
        String from,
        String to,
        long amount,
        long accountSeq,
        String signer,
        Optional<TransferStatus> status,
        long ledgerIndex
) {

    public TransferAuditEvent {
        if (ordinal < 0) {
            throw new IllegalArgumentException("Event ordinal must not be negative");
        }

        Objects.requireNonNull(stage, "stage");
        Objects.requireNonNull(transferId, "transferId");
        Objects.requireNonNull(signatureDigest, "signatureDigest");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        Objects.requireNonNull(signer, "signer");
        Objects.requireNonNull(status, "status");
    }

    public enum Stage {
        CLIENT_REQUEST,
        REPLICATION_SUBMISSION,
        REQUEST_STATUS,
        LEDGER_OUTCOME
    }
}