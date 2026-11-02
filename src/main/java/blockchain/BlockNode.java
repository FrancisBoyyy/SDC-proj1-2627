package blockchain;

import app.messages.Transfer;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.HashSet;
import java.util.Set;

/**
 * One locally accepted block and its link to the known parent.
 * This is local chain metadata. Only Block is sent over the network.
 */
final class BlockNode {

    private final Block block;
    private final String hash;
    private final  @Nullable BlockNode parent;
    private final Set<String> transferIds;

    BlockNode(Block block, @Nullable BlockNode parent) {
        this(block, parent, parent == null ? Set.of() : parent.transferIds());
    }

    BlockNode(Block block, @Nullable BlockNode parent, Set<String> inheritedTransferIds) {
        this.block = Objects.requireNonNull(block, "block");
        Objects.requireNonNull(inheritedTransferIds, "inheritedTransferIds");

        if (block.height() == 0) {
            if (parent != null)
                throw new IllegalArgumentException("Genesis cannot have a parent");

            this.parent = null;
        } else {
            this.parent = Objects.requireNonNull(parent, "Non-genesis block needs a parent");

            if (block.height() != this.parent.height() + 1) {
                throw new IllegalArgumentException(
                        "Block height must be parent height plus one");
            }
            if (!block.parentHash().equals(this.parent.hash())) {
                throw new IllegalArgumentException(
                        "Block parent hash does not match parent node");
            }
        }

        this.hash = block.hash();

        Set<String> branchIds = new HashSet<>(inheritedTransferIds);
        for (Transfer transfer : block.transfers())
            branchIds.add(transfer.id());
        this.transferIds = Set.copyOf(branchIds);
    }

    Block block() { return block; }

    String hash() { return hash; }

    long height() { return block.height(); }

    @Nullable BlockNode parent() { return parent; }

    Set<String> transferIds() { return transferIds; }

    boolean isGenesis() { return parent == null; }
}
