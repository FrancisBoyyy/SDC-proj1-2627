package blockchain;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import app.messages.Transfer;

/**
 * Valid pending transfers that may be included in a future block.
 * Transfers remain in this pool until their block is finalized. A transfer
 * included only in an unfinalized fork may therefore be mined on another fork.
 */
final class TransactionPool {

    private static final Comparator<Transfer> CANDIDATE_ORDER =
            Comparator.comparing(Transfer::from)
                    .thenComparingLong(Transfer::seq)
                    .thenComparing(Transfer::id);

    private final int capacity;
    private final Predicate<Transfer> validTransfer;
    private final Map<String, Transfer> transfersById = new HashMap<>();

    TransactionPool(int capacity, Predicate<Transfer> validTransfer) {
        if (capacity <= 0)
            throw new IllegalArgumentException("Transaction pool capacity must be positive");

        this.capacity = capacity;
        this.validTransfer = Objects.requireNonNull(validTransfer, "validTransfer");
    }

    /**
     * @return true if this transfer was newly added
     */
    boolean add(Transfer transfer) {
        Objects.requireNonNull(transfer, "transfer");

        if (!validTransfer.test(transfer))
            return false;

        String id = transfer.id();

        if (transfersById.containsKey(id) || transfersById.size() >= capacity)
            return false;

        transfersById.put(id, transfer);
        return true;
    }

    /**
     * Selects a stable, deterministic block candidate for the given branch.
     * Existing branch transactions and duplicate account sequence numbers
     * are omitted.
     */
    List<Transfer> candidate(BlockNode branchTip, int maximumTransactions) {
        Objects.requireNonNull(branchTip, "branchTip");

        if (maximumTransactions <= 0)
            throw new IllegalArgumentException("Maximum transaction count must be positive");

        Set<String> includedIds = branchTip.transferIds();
        Set<AccountSequence> selectedSequences = new HashSet<>();
        List<Transfer> candidate = new ArrayList<>();

        transfersById.values().stream()
                .filter(transfer -> !includedIds.contains(transfer.id()))
                .sorted(CANDIDATE_ORDER)
                .forEach(transfer -> {
                    AccountSequence sequence = new AccountSequence(transfer.from(), transfer.seq());

                    if (candidate.size() < maximumTransactions && selectedSequences.add(sequence))
                        candidate.add(transfer);
                });

        return List.copyOf(candidate);
    }

    /**
     * Removes transfers only after their block has become final.
     */
    void removeFinalized(Block block) {
        Objects.requireNonNull(block, "block");

        for (Transfer transfer : block.transfers()) {
            transfersById.remove(transfer.id());
        }
    }

    int size() { return transfersById.size(); }

    boolean contains(String transferId) { return transfersById.containsKey(transferId); }

    private record AccountSequence(String account, long sequence) {}
}