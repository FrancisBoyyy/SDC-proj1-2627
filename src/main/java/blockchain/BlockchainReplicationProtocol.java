package blockchain;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import app.messages.Transfer;
import blockchain.events.BlockMined;
import broadcast.events.SelectiveBroadcastRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponBroadcastDeliver;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastConsumerBabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

/**
 * Blockchain replication: mines blocks, disseminates them using BRB, and
 * hands transfers to the application only after their containing blocks reach
 * the configured confirmation depth. A later reorganization cannot undo them.
 */
public final class BlockchainReplicationProtocol
        implements BroadcastConsumerBabelProtocol {

    private static final Logger LOGGER =
            LogManager.getLogger(BlockchainReplicationProtocol.class);

    private final BlockchainConfig config;
    private final Predicate<Transfer> validTransfer;
    private final BlockchainByzantineProfile byzantineProfile;
    private final boolean miningEnabled; // Set to false for tests where we manually deliver blocks

    private Babel runtime;
    private BlockchainState blockchain;
    private TransactionPool transactionPool;
    private BlockMiner miner;
    private final Set<String> handedOffTransferIds = new HashSet<>();

    private long currentMiningGeneration;

    public BlockchainReplicationProtocol(BlockchainConfig config, Predicate<Transfer> validTransfer) {
        this(config, validTransfer, BlockchainByzantineProfile.NONE, true);
    }

    BlockchainReplicationProtocol(BlockchainConfig config, Predicate<Transfer> validTransfer,
                                  boolean miningEnabled) {
        this(config, validTransfer, BlockchainByzantineProfile.NONE, miningEnabled);
    }

    public BlockchainReplicationProtocol(
            BlockchainConfig config,
            Predicate<Transfer> validTransfer,
            BlockchainByzantineProfile byzantineProfile
    ) {
        this(config, validTransfer, byzantineProfile, true);
    }

    private BlockchainReplicationProtocol(
            BlockchainConfig config,
            Predicate<Transfer> validTransfer,
            BlockchainByzantineProfile byzantineProfile,
            boolean miningEnabled
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.validTransfer = Objects.requireNonNull(
                validTransfer, "validTransfer");
        this.byzantineProfile = Objects.requireNonNull(
                byzantineProfile, "byzantineProfile");
        this.miningEnabled = miningEnabled;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.blockchain = new BlockchainState(config, validTransfer);
        this.transactionPool = new TransactionPool(
                config.maxPendingTransfers(), validTransfer);
        this.miner = new BlockMiner(runtime);

        // Mine empty blocks too. They are needed to give the latest
        // transaction blocks their configured number of confirmations.
        startMining();
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        Transfer transfer = submit.transfer();

        if (transactionPool.add(transfer)) {
            LOGGER.debug("Added transfer {} to the blockchain pool", transfer.id());
            startMining();
        }
    }

    @UponBabelEvent
    public void uponBlockMined(BlockMined event) {
        if (event.generation() != currentMiningGeneration) {
            LOGGER.debug("Ignoring stale mining result generation {}",
                    event.generation());
            return;
        }

        Block block = event.block();

        // Revalidate against current state. Mining may have taken long enough
        // for another block to change the selected tip.
        if (!block.parentHash().equals(blockchain.tip().hash())) {
            LOGGER.debug("Ignoring mined block whose parent is no longer the tip");
            startMining();
            return;
        }

        BlockNode previousTip = blockchain.tip();
        if (!blockchain.addBlock(block)) {
            LOGGER.warn("Locally mined block failed chain validation: {}",
                    block.hash());
            startMining();
            return;
        }

        LOGGER.debug("Mined block {} at height {}",
                block.hash(), block.height());
        logChainSelection(previousTip, blockchain.tip());

        // Insert locally first, then disseminate through BRB. When BRB later
        // delivers this same block locally, addBlock will identify it as known.
        if (byzantineProfile == BlockchainByzantineProfile.WITHHOLD_BLOCKS) {
            LOGGER.warn(
                    "BYZANTINE_ACTION profile={} action=withhold block_hash={} height={}",
                    byzantineProfile,
                    block.hash(),
                    block.height());
        } else if (byzantineProfile == BlockchainByzantineProfile.BROADCAST_INVALID_BLOCK) {
            Block invalidBlock = new Block(
                    "invalid-parent-hash",
                    block.height(),
                    block.transfers(),
                    block.nonce());
            LOGGER.warn(
                    "BYZANTINE_ACTION profile={} action=broadcast_invalid_block "
                            + "valid_block_hash={} sent_block_hash={} sent_parent_hash={} "
                            + "height={} transfer_count={}",
                    byzantineProfile,
                    block.hash(),
                    invalidBlock.hash(),
                    invalidBlock.parentHash(),
                    invalidBlock.height(),
                    invalidBlock.transfers().size());
            broadcast(runtime, invalidBlock);
        } else if (byzantineProfile == BlockchainByzantineProfile.SELECTIVE_SEND) {
            runtime.deliverEvent(new SelectiveBroadcastRequest(block, 1));
        } else {
            broadcast(runtime, block);
        }

        handOffStableBlocks();
        startMining();
    }

    @UponBroadcastDeliver
    public void uponDeliverBlock(Block block, BabelNode origin) {
        BlockNode previousTip = blockchain.tip();
        if (!blockchain.addBlock(block)) {
            // This includes the local miner's own block when BRB delivers it
            // back, repeated delivery, invalid blocks, and stored orphans.
            String reason = block.hasWellFormedParentHash()
                    ? "invalid_duplicate_or_orphan"
                    : "malformed_parent_hash";
            LOGGER.info(
                    "BLOCK_NOT_ACCEPTED reason={} hash={} parent={} height={} origin={}",
                    reason,
                    block.hash(),
                    block.parentHash(),
                    block.height(),
                    origin);
            return;
        }

        LOGGER.debug("Accepted BRB-delivered block {} from {}",
                block.hash(), origin);
        logChainSelection(previousTip, blockchain.tip());

        handOffStableBlocks();
        startMining();
    }

    private void handOffStableBlocks() {
        List<Block> newlyStable = blockchain.advanceStableBlocks();

        for (Block block : newlyStable) {
            for (Transfer transfer : block.transfers()) {
                if (!handedOffTransferIds.add(transfer.id())) {
                    LOGGER.warn(
                            "Stable transfer {} appeared on another selected branch; "
                                    + "not handing it to the payment service twice",
                            transfer.id());
                    continue;
                }
                runtime.deliverEvent(new ExecuteTransaction(transfer));
                LOGGER.debug("Submitted stable transfer {} to the ledger",
                        transfer.id());
            }
            transactionPool.removeFinalized(block);
        }
    }

    private void logChainSelection(BlockNode previousTip, BlockNode selectedTip) {
        if (previousTip.hash().equals(selectedTip.hash()))
            return;

        BlockNode left = previousTip;
        BlockNode right = selectedTip;
        while (left.height() > right.height())
            left = Objects.requireNonNull(left.parent());
        while (right.height() > left.height())
            right = Objects.requireNonNull(right.parent());
        while (!left.hash().equals(right.hash())) {
            left = Objects.requireNonNull(left.parent());
            right = Objects.requireNonNull(right.parent());
        }

        if (left.height() < previousTip.height()) {
            LOGGER.warn(
                    "CHAIN_REORG old_tip={} old_height={} new_tip={} new_height={} "
                            + "common_ancestor={} fork_height={}",
                    previousTip.hash(), previousTip.height(),
                    selectedTip.hash(), selectedTip.height(),
                    left.hash(), left.height());
        } else {
            LOGGER.debug("CHAIN_EXTEND old_tip={} new_tip={} height={}",
                    previousTip.hash(), selectedTip.hash(), selectedTip.height());
        }
    }

    private void startMining() {
        if (!miningEnabled)
            return;

        BlockNode tip = blockchain.tip();
        List<Transfer> candidate = transactionPool.candidate(
                tip, config.maxTransactionsPerBlock());

        currentMiningGeneration = miner.mine(
                tip.hash(),
                tip.height() + 1,
                candidate,
                config.difficultyBits());
    }

    @Override
    public void stop() {
        if (miner != null)
            miner.close();
    }
}
