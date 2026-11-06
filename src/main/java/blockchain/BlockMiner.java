package blockchain;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import app.messages.Transfer;
import blockchain.events.BlockMined;
import pt.unl.fct.di.novasys.babel2.core.Babel;

/**
 * Performs proof-of-work outside the Babel protocol handler thread.
 */
final class BlockMiner implements AutoCloseable {

    private final Babel runtime;
    private final LongSupplier nonceStartSupplier;
    private final ExecutorService worker =
            Executors.newSingleThreadExecutor(task -> {
                Thread thread = new Thread(task, "block-miner");
                thread.setDaemon(true);
                return thread;
            });
    private final AtomicLong currentGeneration = new AtomicLong();

    BlockMiner(Babel runtime) {
        this(runtime, () -> ThreadLocalRandom.current().nextLong());
    }

    BlockMiner(Babel runtime, LongSupplier nonceStartSupplier) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.nonceStartSupplier = Objects.requireNonNull(
                nonceStartSupplier, "nonceStartSupplier");
    }

    /**
     * Starts mining an immutable snapshot and returns its generation number.
     * A later start or cancel invalidates this request.
     */
    long mine(String parentHash, long height, List<Transfer> transactions, int difficultyBits) {
        Objects.requireNonNull(parentHash, "parentHash");
        Objects.requireNonNull(transactions, "transactions");

        if (height <= 0)
            throw new IllegalArgumentException("Mined block height must be positive");
        if (difficultyBits < 0 || difficultyBits > Block.HASH_BYTES * Byte.SIZE)
            throw new IllegalArgumentException("Difficulty must be between 0 and 256 bits");

        // Block copies the list and its transfer signatures, so later pool
        // changes cannot alter the work being performed.
        Block candidate = new Block(parentHash, height, transactions, 0);
        long generation = currentGeneration.incrementAndGet();
        // Avoid having miners with identical candidates repeatedly solve the
        // same proof of work from nonce zero.
        long nonceStart = nonceStartSupplier.getAsLong();

        worker.submit(() -> search(candidate, difficultyBits, generation, nonceStart));
        return generation;
    }

    /**
     * Invalidates active and queued mining work.
     */
    long cancel() { return currentGeneration.incrementAndGet(); }

    private void search(
            Block candidate,
            int difficultyBits,
            long generation,
            long nonceStart
    ) {
        long nonce = nonceStart;
        while (isCurrent(generation) && !Thread.currentThread().isInterrupted()) {
            Block attempt = candidate.withNonce(nonce);

            if (attempt.hasValidProofOfWork(difficultyBits)) {
                if (isCurrent(generation))
                    runtime.deliverEvent(new BlockMined(generation, attempt));
                return;
            }

            // Avoid overflowing from Long.MAX_VALUE back to Long.MIN_VALUE.
            if (nonce == Long.MAX_VALUE)
                return;

            nonce++;
        }
    }

    private boolean isCurrent(long generation) { return currentGeneration.get() == generation; }

    @Override
    public void close() { cancel(); worker.shutdownNow(); }
}
