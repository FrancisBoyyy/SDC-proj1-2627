package blockchain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import blockchain.events.BlockMined;
import pt.unl.fct.di.novasys.babel2.core.Babel;

class BlockMinerTest {

    private static Babel runtime(BlockingQueue<BlockMined> results) {
        return Babel.builder()
                .onDeliver(BlockMined.class, results::add)
                .build()
                .init();
    }

    @Test
    void minesAndReportsBlockThroughBabelEventDelivery() throws Exception {
        BlockingQueue<BlockMined> results = new LinkedBlockingQueue<>();
        Babel runtime = runtime(results);

        try (var miner = new BlockMiner(runtime)) {
            long generation = miner.mine(Block.genesis().hash(), 1, List.of(), 0);
            BlockMined result = results.poll(2, TimeUnit.SECONDS);

            assertTrue(result != null, "Miner did not report within the timeout");
            assertEquals(generation, result.generation());
            assertEquals(Block.genesis().hash(), result.block().parentHash());
            assertEquals(1, result.block().height());
            assertTrue(result.block().hasValidProofOfWork(0));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void cancellationPreventsOldGenerationFromReporting() throws Exception {
        BlockingQueue<BlockMined> results = new LinkedBlockingQueue<>();
        Babel runtime = runtime(results);

        try (var miner = new BlockMiner(runtime)) {
            long oldGeneration = miner.mine(Block.genesis().hash(), 1, List.of(), 256);
            long cancelledGeneration = miner.cancel();
            long newGeneration = miner.mine(Block.genesis().hash(), 1, List.of(), 0);

            assertTrue(cancelledGeneration > oldGeneration);
            assertTrue(newGeneration > cancelledGeneration);

            BlockMined result = results.poll(2, TimeUnit.SECONDS);
            assertTrue(result != null, "Current mining generation did not report");
            assertEquals(newGeneration, result.generation());
            assertFalse(results.stream().anyMatch(event -> event.generation() == oldGeneration));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void newerMiningRequestMakesEarlierResultStale() throws Exception {
        BlockingQueue<BlockMined> results = new LinkedBlockingQueue<>();
        Babel runtime = runtime(results);

        try (var miner = new BlockMiner(runtime)) {
            long first = miner.mine(Block.genesis().hash(), 1, List.of(), 256);
            long second = miner.mine(Block.genesis().hash(), 2, List.of(), 0);

            BlockMined result = results.poll(2, TimeUnit.SECONDS);
            assertTrue(result != null, "Latest mining request did not report");
            assertEquals(second, result.generation());
            assertTrue(second > first);
            assertEquals(2, result.block().height());
            assertFalse(results.stream().anyMatch(event -> event.generation() == first));
        } finally {
            runtime.shutdown();
        }
    }

    @Test
    void rejectsInvalidMiningParameters() {
        BlockingQueue<BlockMined> results = new LinkedBlockingQueue<>();
        Babel runtime = runtime(results);

        try (var miner = new BlockMiner(runtime)) {
            assertThrows(NullPointerException.class,
                    () -> miner.mine(null, 1, List.of(), 0));
            assertThrows(NullPointerException.class,
                    () -> miner.mine(Block.genesis().hash(), 1, null, 0));
            assertThrows(IllegalArgumentException.class,
                    () -> miner.mine(Block.genesis().hash(), 0, List.of(), 0));
            assertThrows(IllegalArgumentException.class,
                    () -> miner.mine(Block.genesis().hash(), 1, List.of(), -1));
            assertThrows(IllegalArgumentException.class,
                    () -> miner.mine(Block.genesis().hash(), 1, List.of(), 257));
        } finally {
            runtime.shutdown();
        }
    }
}
