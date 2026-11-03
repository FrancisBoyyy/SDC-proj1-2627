package blockchain;

import app.messages.Transfer;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;

/**
 * Local block tree, fork choice, orphan handling, and confirmation tracking.
 * Calls are expected to be serialized by the owning Babel protocol.
 */
final class BlockchainState {

    private final BlockchainConfig config;
    private final Predicate<Transfer> validTransfer;
    private final Map<String, BlockNode> blocksByHash = new HashMap<>();
    private final Map<String, List<Block>> orphansByParent = new HashMap<>();
    private final LinkedHashMap<String, Block> orphansByHash = new LinkedHashMap<>();
    private final Set<String> handedOffStableBlockHashes = new HashSet<>();

    private final BlockNode genesis;
    private BlockNode tip;

    BlockchainState(BlockchainConfig config, Predicate<Transfer> validTransfer) {
        this.config = Objects.requireNonNull(config, "config");
        this.validTransfer = Objects.requireNonNull(validTransfer, "validTransfer");

        genesis = new BlockNode(Block.genesis(), null);
        tip = genesis;
        blocksByHash.put(genesis.hash(), genesis);
    }

    BlockNode genesis() { return genesis; }

    BlockNode tip() { return tip; }

    /**
     * Inserts a valid block. A block whose parent is not known is retained as
     * an orphan, subject to the configured bound, and reconsidered when its
     * parent arrives. The return value reports whether this block entered the
     * connected block tree (directly or by orphan resolution).
     */
    boolean addBlock(Block block) {
        if (!hasValidBasicFields(block))
            return false;

        String hash = block.hash();
        if (blocksByHash.containsKey(hash) || orphansByHash.containsKey(hash))
            return false;

        BlockNode parent = blocksByHash.get(block.parentHash());
        if (parent == null) {
            rememberOrphan(block);
            return false;
        }

        if (!hasValidContext(block, parent))
            return false;

        BlockNode node;
        try {
            node = new BlockNode(block, parent, parent.transferIds());
        } catch (IllegalArgumentException e) {
            return false;
        }

        blocksByHash.put(node.hash(), node);
        updateTip(node);
        attachOrphans(node.hash());
        return true;
    }

    /**
     * Returns blocks on the current longest valid chain that have reached k
     * confirmations and have not previously been handed to the application.
     *
     * <p>Blocks on a competing fork remain eligible even when they conflict
     * with a block handed off earlier. The payment service does not support
     * rollback, so a deep reorganization can leave irreversible application
     * effects from both branches.
     */
    List<Block> advanceStableBlocks() {
        long targetHeight = tip.height() - config.confirmations();
        if (targetHeight <= 0)
            return List.of();

        BlockNode stableTip = tip;
        while (stableTip.height() > targetHeight)
            stableTip = Objects.requireNonNull(stableTip.parent());

        List<Block> newlyStable = new ArrayList<>();
        BlockNode current = stableTip;
        while (current.height() > 0) {
            if (!handedOffStableBlockHashes.contains(current.hash()))
                newlyStable.add(current.block());
            current = Objects.requireNonNull(current.parent());
        }
        Collections.reverse(newlyStable);
        for (Block block : newlyStable)
            handedOffStableBlockHashes.add(block.hash());
        return List.copyOf(newlyStable);
    }

    private boolean hasValidBasicFields(Block block) {
        if (block == null || block.height() <= 0 || !block.hasWellFormedParentHash())
            return false;
        if (block.transfers().size() > config.maxTransactionsPerBlock())
            return false;
        try {
            return block.hasValidProofOfWork(config.difficultyBits());
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean hasValidContext(Block block, BlockNode parent) {
        if (parent.height() == Long.MAX_VALUE
                || block.height() != parent.height() + 1
                || !block.parentHash().equals(parent.hash()))
            return false;

        Set<String> idsInBlock = new HashSet<>();
        for (Transfer transfer : block.transfers()) {
            String id = transfer.id();
            if (!validTransfer.test(transfer)
                    || !idsInBlock.add(id)
                    || parent.transferIds().contains(id))
                return false;
        }
        return true;
    }

    private void updateTip(BlockNode candidate) {
        if (candidate.height() > tip.height()
                || (candidate.height() == tip.height()
                    && candidate.hash().compareTo(tip.hash()) < 0))
            tip = candidate;
    }

    private void rememberOrphan(Block block) {
        if (config.maxOrphans() == 0)
            return;

        orphansByHash.put(block.hash(), block);
        orphansByParent.computeIfAbsent(block.parentHash(), ignored -> new ArrayList<>()).add(block);
        while (orphansByHash.size() > config.maxOrphans()) {
            String oldestHash = orphansByHash.keySet().iterator().next();
            removeOrphan(oldestHash);
        }
    }

    private void removeOrphan(String hash) {
        Block removed = orphansByHash.remove(hash);
        if (removed == null)
            return;

        List<Block> siblings = orphansByParent.get(removed.parentHash());
        if (siblings != null) {
            siblings.removeIf(block -> block.hash().equals(hash));
            if (siblings.isEmpty())
                orphansByParent.remove(removed.parentHash());
        }
    }

    private void attachOrphans(String parentHash) {
        List<Block> waiting = orphansByParent.remove(parentHash);
        if (waiting == null)
            return;

        for (Block orphan : waiting) {
            orphansByHash.remove(orphan.hash());
            addBlock(orphan);
        }
    }
}
