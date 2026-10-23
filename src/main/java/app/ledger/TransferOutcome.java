package app.ledger;

import app.messages.Transfer;

/**
 * Outcome of a transfer.
 *
 * @param ledgerIndex position of the transfer in the (local) ledger if it was
 *                    executed, -1 otherwise
 */
public record TransferOutcome(Transfer transfer, TransferStatus status, long ledgerIndex) {
}
