package blockchain;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import app.messages.Transfer;
import blockchain.events.BlockMined;
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
 * executes transfers only after their containing blocks are finalized.
 */
public final class BlockchainReplicationProtocol
        implements BroadcastConsumerBabelProtocol {

    private static final Logger LOGGER =
            LogManager.getLogger(BlockchainReplicationProtocol.class);

    private final BlockchainConfig config;
    private final Predicate<Transfer> validTransfer;

    private Babel runtime;
    private BlockchainState blockchain;
    private TransactionPool transactionPool;
    private BlockMiner miner;

    private long currentMiningGeneration;

    public BlockchainReplicationProtocol(
            BlockchainConfig config,
            Predicate<Transfer> validTransfer
    ) {
        this.config = Objects.requireNonNull(config, "config");
        this.validTransfer = Objects.requireNonNull(
                validTransfer, "validTransfer");
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

        if (!blockchain.addBlock(block)) {
            LOGGER.warn("Locally mined block failed chain validation: {}",
                    block.hash());
            startMining();
            return;
        }

        LOGGER.debug("Mined block {} at height {}",
                block.hash(), block.height());

        // Insert locally first, then disseminate through BRB. When BRB later
        // delivers this same block locally, addBlock will identify it as known.
        broadcast(runtime, block);

        finalizeStableBlocks();
        startMining();
    }

    @UponBroadcastDeliver
    public void uponDeliverBlock(Block block, BabelNode origin) {
        if (!blockchain.addBlock(block)) {
            // This includes the local miner's own block when BRB delivers it
            // back, repeated delivery, invalid blocks, and stored orphans.
            return;
        }

        LOGGER.debug("Accepted BRB-delivered block {} from {}",
                block.hash(), origin);

        finalizeStableBlocks();
        startMining();
    }

    private void finalizeStableBlocks() {
        List<Block> newlyFinalized = blockchain.advanceFinality();

        for (Block block : newlyFinalized) {
            for (Transfer transfer : block.transfers()) {
                runtime.deliverEvent(new ExecuteTransaction(transfer));
                LOGGER.debug("Submitted finalized transfer {} to the ledger",
                        transfer.id());
            }
            transactionPool.removeFinalized(block);
        }
    }

    private void startMining() {
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
