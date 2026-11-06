package app.ledger;

/**
 * Possible outcomes of handing a transfer to the {@link Ledger}.
 */
public enum TransferStatus {
    /** The transfer was executed. */
    EXECUTED,
    /**
     * The transfer is valid but cannot be executed yet: either some previous
     * transfer of the same account was not executed yet, or the account does not
     * have enough funds (yet).
     */
    PENDING,
    /**
     * The transfer is invalid: unknown or equal accounts, non positive amount,
     * negative sequence number, the signer is not an owner of the source account,
     * or the signature is not valid.
     */
    INVALID,
    /** The same transfer was already handed to the ledger (it is pending, or it was already executed). */
    DUPLICATE,
    /**
     * A different transfer of the same account with the same sequence number was
     * handed to the ledger first. This transfer will never be executed.
     */
    CONFLICT
}
