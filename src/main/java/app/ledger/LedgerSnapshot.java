package app.ledger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Immutable view of the ledger state, used by correctness checks and
 * experiment result collection.
 */
public record LedgerSnapshot(
        Map<String, AccountState> accounts,
        long totalSupply,
        long sumOfBalances,
        String stateDigest
) {

    public LedgerSnapshot {
        Objects.requireNonNull(accounts, "accounts");
        Objects.requireNonNull(stateDigest, "stateDigest");

        // Ordered accounts makes snapshots easier to compare and inspect.
        accounts = Collections.unmodifiableMap(new TreeMap<>(accounts));
    }

    /**
     * @return IDs of all transfers that have been executed
     */
    public Set<String> executedTransferIds() {
        Set<String> ids = new TreeSet<>();
        accounts.values().forEach(account ->
                ids.addAll(account.executedTransferIds()));
        return Collections.unmodifiableSet(ids);
    }

    /**
     * @return IDs of all transfers currently pending in the ledger
     */
    public Set<String> pendingTransferIds() {
        Set<String> ids = new TreeSet<>();
        accounts.values().forEach(account ->
                ids.addAll(account.pendingTransferIds()));
        return Collections.unmodifiableSet(ids);
    }

    public boolean supplyConserved() {
        return totalSupply == sumOfBalances;
    }

    /**
     * Account state includes transfer IDs in account sequence order.
     */
    public record AccountState(
            long balance,
            long nextSeq,
            List<String> executedTransferIds,
            List<String> pendingTransferIds
    ) {
        public AccountState {
            executedTransferIds = List.copyOf(executedTransferIds);
            pendingTransferIds = List.copyOf(pendingTransferIds);
        }
    }
}