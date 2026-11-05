package agreement;

import agreement.events.Decided;
import agreement.events.Propose;
import agreement.events.SuspectPrimary;
import agreement.events.ViewChanged;
import agreement.messages.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.PublicKeyBabelNode;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.BabelProtocol;
import replication.timers.FetchTimer;
import utils.Crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PracticalByzantineFaultTolerantProtocol implements BabelProtocol {

    private static final Logger LOGGER = LogManager.getLogger(PracticalByzantineFaultTolerantProtocol.class);

    private final List<BabelNode> neighbors;

    private final int prepareQuorum;
    private final int commitQuorum;
    private final int newViewQuorum;
    private final int amplificationQuorum;

    private final Map<PBFTInstanceId, PBFTState> instances;

    private long currentView;
    private long currentSeqn; // monotonic increasing sequence number, only for primary usage!

    private boolean inViewChange;
    private long pendingView; // the view wanted to move to
    private final Map<Long, Map<BabelNode, ViewChangeMsg>> viewChanges;
    private @Nullable ScheduledFuture<?> viewChangeTimer;

    // CHECKPOINT
    private static final long CHECKPOINT_INTERVAL = 50; // K
    private static final long LOG_SIZE = 2 * CHECKPOINT_INTERVAL; // L (multiple of K): water marks are (h, h + L]
    private final int checkpointQuorum; // = 2f + 1, set in the constructor
    private long lastStableSeqn = 0;
    private List<CheckpointMsg> stableProof = List.of();
    private long executedUpTo = 0; // highest contiguous decided seqn
    private String chainDigest = ""; // digest of the decided history up to executedUpTo
    private final Map<Long, String> decidedDigests = new HashMap<>(); // decided but not yet "executed"
    private final Map<Long, Map<BabelNode, CheckpointMsg>> checkpointVotes = new HashMap<>();

    // CHECKPOINT STATE TRANSFER
    private static final long RETAIN = 3 * CHECKPOINT_INTERVAL;
    private static final long FETCH_RANGE_CAP = RETAIN;
    private static final long FETCH_GRACE_MS = 300;
    private static final long FETCH_RETRY_MS = 1000;
    private long lastCollected = 0;
    private @Nullable ScheduledFuture<?> fetchTimer;
    private final TreeMap<Long, List<BabelMessage>> decidedLog = new TreeMap<>();  // retained for serving fetches
    private final Deque<List<BabelMessage>> deferredBatches = new ArrayDeque<>(); // primary: waiting for window space
    private long transferTarget = 0;        // 0 = no transfer in progress
    private String transferDigest;
    private List<BabelNode> transferPeers;  // the signers of the stable proof
    private int transferAttempt;

    /**
     * Relevant objects to communicate with the Babel 2 framework.
     * The runtime object is for intra machine protocol communication. The channel
     * object is for inter machine communication.
     * The Babel object can only have one implementation. while BabelChannel can
     * have many. Ideally, the protocol code should not know what is the specific
     * BabelChannel implementation being used, only what guarantees it provides.
     */
    private Babel runtime;
    private BabelChannel channel;

    public PracticalByzantineFaultTolerantProtocol(List<BabelNode> membership, int f) {
        if (f < 0 || membership.isEmpty() || membership.size() < 3 * f + 1)
            throw new IllegalArgumentException(
                    "Require distinct replicas, f >= 0, and N >= 3f + 1");

        this.neighbors = new ArrayList<>(membership);

        this.prepareQuorum = 2*f + 1; // the implementation includes the primary in the quorum threshold
        this.commitQuorum = 2*f + 1;
        this.newViewQuorum = 2*f + 1;
        this.amplificationQuorum = f + 1;

        this.instances = new HashMap<>();

        this.currentView = 0; // by convention!
        this.currentSeqn = 0; // by convention!

        this.inViewChange = false;
        this.viewChanges = new HashMap<>();

        this.checkpointQuorum = 2*f + 1;
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        if (!neighbors.contains(runtime.getMyself())) {
            throw new IllegalStateException(
                    "This process is not a configured replica");
        }
        this.runtime = runtime;
        this.channel = channel;
    }

    // auxiliary methods

    /**
     * Sends the given to the membership excluding the given node.
     */
    private void sendToNeighbors(BabelMessage message, @Nullable BabelNode exclude) {
        for (BabelNode node : neighbors)
            if (!node.equals(exclude))
                channel.send(node, message);
    }

    /**
     * Sends the given message to all of the membership (including the sender).
     */
    private void sendToAll(BabelMessage message) {
        sendToNeighbors(message, null);
    }

    /**
     * Sends the given message to the set of neighbors of this replica
     * (excluding the replica itself...).
     */
    private void sendToOthers(BabelMessage m) {
        sendToNeighbors(m, runtime.getMyself());
    }

    /**
     * Returns the node expected to be the primary of the given view.
     */
    private BabelNode expectedPrimary(long view) {
        return neighbors.get(Math.floorMod(view, neighbors.size()));
    }

    /**
     * Returns the bytes contents of the given (v, s, d) tuple.
     */
    private static byte[] bytesOf(String tag, long view, long seqn, String digest) {
        return String.join("|", tag, String.valueOf(view), String.valueOf(seqn), digest).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] prePrepareBytes(long view, long seqn, String digest) {
        return bytesOf("PrePrepare", view, seqn, digest);
    }
    private static byte[] prepareBytes(long view, long seqn, String digest) {
        return bytesOf("Prepare", view, seqn, digest);
    }

    private static byte[] commitBytes(long view, long seqn, String digest) {
        return bytesOf("Commit", view, seqn, digest);
    }

    private boolean isObsolete(long seqn) {
        return seqn <= Math.min(lastStableSeqn, executedUpTo);
    }

    /**
     * High water mark H = h + L, h being the last stable checkpoint. Only the primary enforces it (throttling its
     * proposals): a backup that is slower to stabilize a checkpoint would otherwise silently drop PrePrepares the
     * primary legitimately sent, and nothing retransmits them.
     */
    private boolean aboveHighWaterMark(long seqn) {
        return seqn > lastStableSeqn + LOG_SIZE;
    }

    /**
     * Returns the consensus instance for the given view and sequence number.
     */
    private PBFTState stateFor(long view, long seqn) {
        PBFTInstanceId id = new PBFTInstanceId(view, seqn);
        return instances.computeIfAbsent(id, ignored -> new PBFTState());
    }

    // NOTIFICATIONS

    /**
     * Handles the propose, made by the primary.
     * Selects a monotonic increasing sequence number for the new batch to be ordered across replicas.
     * Sets the primary state has PrePrepared, since it is not a backup to receive any PrePrepares.
     * Builds the PrePrepare message containing the client request, that sequence number and
     * the batch contents digest, signed by the primary, sending it to all replicas
     */
    @UponBabelEvent
    public void uponPropose(Propose propose) throws GeneralSecurityException {
        // dropped batches are re-proposed by the SMR layer upon ViewChanged
        if (inViewChange || !expectedPrimary(currentView).equals(runtime.getMyself()))
            return;

        deferredBatches.add(propose.batch());
        proposeDeferred();
    }

    /**
     * Proposes the batches that fit in the water mark window; the rest wait for the next stable checkpoint.
     */
    private void proposeDeferred() throws GeneralSecurityException {
        if (inViewChange || !expectedPrimary(currentView).equals(runtime.getMyself()))
            return;

        List<BabelMessage> batch;
        while (!aboveHighWaterMark(currentSeqn + 1) && (batch = deferredBatches.poll()) != null)
            propose(batch);
    }

    private void propose(List<BabelMessage> batch) throws GeneralSecurityException {
        String hash = PBFTState.batchDigest(batch);
        currentSeqn++;
        byte[] sig = Crypto.sign(prePrepareBytes(currentView, currentSeqn, hash), runtime.getKeys().getPrivate());
        PrePrepareMessage preprepareMsg = new PrePrepareMessage(batch, currentView, currentSeqn, hash, sig);

        PBFTState state = stateFor(preprepareMsg.view(), preprepareMsg.seqn());
        state.setPrePrepared(preprepareMsg, runtime.getMyself());

        sendToOthers(preprepareMsg);

        LOGGER.info("Receive propose myself (primary) for batch... sending to current membership with (v, n, d)=({}, {}, {})",
                currentView, currentSeqn, hash);
    }

    /**
     * Handles the PrePrepare message.
     * A PrePrepare is accepted by a replica if it meets the following criteria:
     * - the sender must be the primary in the current view
     * - the current view must be equal to the message's view
     * - the message's digest must be equal to the hash of the batch's contents
     * - there is no other PrePrepare message accepted with a different digest
     * - the signature of the message can be verified with the primary's public key
     * If all criteria is met, the replica constructs a Prepare message, registers its own Prepare
     * and forwards it to all its neighbors.
     */
    @UponMessageIn
    public void uponPrePrepareMsg(PrePrepareMessage msg, BabelNode sender) throws GeneralSecurityException {
        if (inViewChange)
            return;

        if (isObsolete(msg.seqn())) return;

        LOGGER.info("Pre-prepare received from {}", sender);

        if (currentView != msg.view()) {
            LOGGER.warn("currentView={} != msg.view={}", currentView, msg.view());
            return;
        }

        if (!sender.equals(expectedPrimary(currentView))) {
            LOGGER.warn("sender={} is not the primary={}", sender, expectedPrimary(currentView));
            return;
        }

        String expectedHash = PBFTState.batchDigest(msg.ms());
        if (!msg.hash().equals(expectedHash)) {
            LOGGER.warn("msg.hash()={} != expectedHash={}", msg.hash(), expectedHash);
            return;
        }

        PBFTState state = stateFor(msg.view(), msg.seqn());
        if (!state.canAcceptPrePrepare(expectedHash)) {
            LOGGER.warn("This replica previously accept a PrePrepare with a different hash.");
            return;
        }

        // verify the signature
        if (!Crypto.verify(prePrepareBytes(msg.view(), msg.seqn(), expectedHash),
                msg.sig(),
                ((PublicKeyBabelNode) sender).identity())
        ) {
            LOGGER.warn("The signature is not verifiable with the sender's public key.");
            return;
        }

        // all conditions verified -> accept the pre-prepare!
        state.setPrePrepared(msg, sender);

        byte[] sig = Crypto.sign(prepareBytes(msg.view(), msg.seqn(), expectedHash), runtime.getKeys().getPrivate());
        PrepareMessage prepare = new PrepareMessage(msg.view(), msg.seqn(), expectedHash, sig);
        LOGGER.info("Accepted PrePrepare -> Prepare {} sent", prepare);
        state.registerPrepare(runtime.getMyself(), prepare); // own prepare
        sendToOthers(prepare);

        tryPrepared(state, msg.view(), msg.seqn()); // because other prepares may be buffered
    }

    /**
     * Tries to reach a prepare byzantine quorum of replicas.
     * If it is reached, send the corresponding commit to the replica neighbors,
     * registering the commit locally, also setting the consensus instance as prepared.
     */
    private void tryPrepared(PBFTState state, long view, long seqn) throws GeneralSecurityException {
        if (state.getPrepareSize() < prepareQuorum)
            return;

        state.setPrepared();

        byte[] sig = Crypto.sign(commitBytes(view, seqn, state.getDigest()), runtime.getKeys().getPrivate());
        CommitMessage commit = new CommitMessage(view, seqn, state.getDigest(), sig);
        LOGGER.info("Prepares quorum reached -> Commit {} sent", commit);
        state.registerCommit(runtime.getMyself(), commit);
        sendToOthers(commit);

        tryCommitted(state, seqn);
    }


    /**
     * Handles receiving a Prepare message.
     * Registers it and tries to reach a quorum to progress in the protocol.
     */
    @UponMessageIn
    public void uponPrepareMsg(PrepareMessage msg, BabelNode sender) throws GeneralSecurityException {
        if (sender.equals(expectedPrimary(msg.view())) || inViewChange)
            return;   // the primary's vote is its PrePrepare

        if (isObsolete(msg.seqn())) return;

        // verify the signature
        if (!Crypto.verify(prepareBytes(msg.view(), msg.seqn(), msg.hash()),
                msg.sig(),
                ((PublicKeyBabelNode) sender).identity())
        ) {
            LOGGER.warn("The signature is not verifiable with the sender's public key.");
            return;
        }

        PBFTState state = stateFor(msg.view(), msg.seqn());
        state.registerPrepare(sender, msg);
        tryPrepared(state, msg.view(), msg.seqn());
    }

    @UponMessageIn
    public void uponCommitMsg(CommitMessage msg, BabelNode sender) throws GeneralSecurityException {
        if (inViewChange)
            return;

        if (isObsolete(msg.seqn())) return;

        // verify the signature
        if (!Crypto.verify(commitBytes(msg.view(), msg.seqn(), msg.hash()),
                msg.sig(),
                ((PublicKeyBabelNode) sender).identity())
        )
            return;

        PBFTState state = stateFor(msg.view(), msg.seqn());
        state.registerCommit(sender, msg);
        tryCommitted(state, msg.seqn());
    }

    @UponMessageIn
    public void uponFetchMsg(FetchMsg msg, BabelNode sender) {
        if (!neighbors.contains(sender) || sender.equals(runtime.getMyself())
                || msg.fromSeqn() < 1 || msg.toSeqn() < msg.fromSeqn()
                || msg.toSeqn() - msg.fromSeqn() >= FETCH_RANGE_CAP) return;

        var batches = new ArrayList<List<BabelMessage>>();
        for (long s = msg.fromSeqn(); s <= msg.toSeqn(); s++) {
            List<BabelMessage> batch = decidedLog.get(s);
            if (batch == null) {
                LOGGER.warn("Cannot serve fetch [{}, {}] from replica {}: seqn {} is not in my log",
                        msg.fromSeqn(), msg.toSeqn(), neighbors.indexOf(sender), s);
                return;
            }
            batches.add(batch);
        }
        LOGGER.info("Serving fetch [{}, {}] to replica {}", msg.fromSeqn(), msg.toSeqn(), neighbors.indexOf(sender));
        channel.send(sender, new BatchesMsg(msg.fromSeqn(), batches));
    }

    @UponMessageIn
    public void uponBatchesMsg(BatchesMsg msg, BabelNode sender) throws GeneralSecurityException {
        if (transferTarget == 0 || !neighbors.contains(sender) || msg.batches().isEmpty()
                || msg.fromSeqn() != executedUpTo + 1
                || msg.fromSeqn() + msg.batches().size() - 1 != transferTarget) return;

        String chain = chainDigest;
        for (List<BabelMessage> batch : msg.batches())
            chain = chainStep(chain, PBFTState.batchDigest(batch));
        if (!chain.equals(transferDigest)) {
            LOGGER.warn("Fetched batches from replica {} do not match the stable digest, trying another peer",
                    neighbors.indexOf(sender));
            requestFetch();
            return;
        }

        long seqn = msg.fromSeqn();
        for (List<BabelMessage> batch : msg.batches()) {
            runtime.deliverEvent(new Decided(seqn, batch)); // SMR ignores what it already has
            decidedLog.put(seqn++, batch);
        }
        long reached = transferTarget;
        executedUpTo = reached;
        chainDigest = chain;
        stopTransfer();
        LOGGER.info("State transfer complete: executed up to {}, chain digest {}", reached, chain);

        advanceCheckpoints();
    }

    private void garbageCollect(long seqn) {
        if (seqn <= lastCollected) return;
        lastCollected = seqn;
        instances.keySet().removeIf(id -> id.seqn() <= seqn);
        decidedDigests.keySet().removeIf(s -> s <= seqn);
        decidedLog.headMap(seqn - RETAIN, true).clear();
    }

    private void tryCommitted(PBFTState state, long seqn) throws GeneralSecurityException {
        if (state.getCommitSize() < commitQuorum)
            return;

        state.setCommited();

        // already executed, or already decided in an earlier view (re-agreed through O)
        if (seqn <= executedUpTo || decidedDigests.putIfAbsent(seqn, state.getDigest()) != null)
            return;

        Decided decided = new Decided(seqn, state.getBatch());
        LOGGER.info("Commits quorum reached -> Decide {} sent to SMR", decided);
        runtime.deliverEvent(decided);
        decidedLog.put(seqn, state.getBatch());
        advanceCheckpoints();
    }

    private static byte[] checkpointBytes(long seqn, String digest) {
        return ("Checkpoint|" + seqn + "|" + digest).getBytes(StandardCharsets.UTF_8);
    }

    /** Extends the digest of the decided history with the digest of the next decided batch. */
    private static String chainStep(String chain, String batchDigest) {
        return Crypto.toHex(Crypto.digest((chain + "|" + batchDigest).getBytes(StandardCharsets.UTF_8)));
    }

    private void advanceCheckpoints() throws GeneralSecurityException {
        String d;
        while ((d = decidedDigests.remove(executedUpTo + 1)) != null) {
            executedUpTo++;
            chainDigest = chainStep(chainDigest, d);
            if (executedUpTo % CHECKPOINT_INTERVAL == 0) {
                byte[] sig = Crypto.sign(checkpointBytes(executedUpTo, chainDigest), runtime.getKeys().getPrivate());
                CheckpointMsg cp = new CheckpointMsg(executedUpTo, chainDigest, runtime.getMyself(), sig);
                registerCheckpoint(cp);
                sendToOthers(cp);
            }
        }
        if (transferTarget != 0 && executedUpTo >= transferTarget) stopTransfer();   // caught up on our own
        garbageCollect(Math.min(lastStableSeqn, executedUpTo));
        startStateTransfer();
    }

    @UponMessageIn
    public void uponCheckpointMsg(CheckpointMsg msg, BabelNode sender) {
        if (!sender.equals(msg.sender()) || !neighbors.contains(sender)
                || msg.seqn() <= lastStableSeqn || msg.seqn() % CHECKPOINT_INTERVAL != 0)
            return;
        if (!Crypto.verify(checkpointBytes(msg.seqn(), msg.digest()), msg.sig(),
                ((PublicKeyBabelNode) sender).identity()))
            return;
        registerCheckpoint(msg);   // deliberately not gated by inViewChange
    }

    private void registerCheckpoint(CheckpointMsg cp) {
        if (cp.seqn() <= lastStableSeqn) return;
        var votes = checkpointVotes.computeIfAbsent(cp.seqn(), k -> new HashMap<>());
        votes.putIfAbsent(cp.sender(), cp);

        List<CheckpointMsg> matching = votes.values().stream()
                .filter(c -> c.digest().equals(cp.digest())).toList();
        if (matching.size() >= checkpointQuorum)
            makeStable(cp.seqn(), matching);
    }

    private void makeStable(long seqn, List<CheckpointMsg> proof) {
        if (seqn <= lastStableSeqn) return;
        LOGGER.info("Stable checkpoint at seqn {} (executed up to {})", seqn, executedUpTo);
        lastStableSeqn = seqn;
        stableProof = List.copyOf(proof);
        checkpointVotes.keySet().removeIf(s -> s <= seqn);

        if (executedUpTo >= seqn) garbageCollect(seqn);
        else startStateTransfer();

        try {
            proposeDeferred(); // the window moved
        } catch (GeneralSecurityException e) {
            LOGGER.error("Could not propose deferred batches", e);
        }
    }

    private void startStateTransfer() {
        if (transferTarget != 0 || executedUpTo >= lastStableSeqn) return;
        LOGGER.info("Behind stable checkpoint {} (executed up to {}): starting state transfer", lastStableSeqn, executedUpTo);
        transferTarget = lastStableSeqn;
        transferDigest = stableProof.get(0).digest();
        transferPeers = stableProof.stream().map(CheckpointMsg::sender).toList();
        transferAttempt = 0;
        armFetchTimer(FETCH_GRACE_MS);   // fetch only if we have not caught up by then
    }

    private void requestFetch() {
        BabelNode peer;
        do {
            peer = transferPeers.get(transferAttempt++ % transferPeers.size());
        } while (peer.equals(runtime.getMyself()));

        LOGGER.info("Fetching seqns [{}, {}] from replica {} (attempt {})",
                executedUpTo + 1, transferTarget, neighbors.indexOf(peer), transferAttempt);
        channel.send(peer, new FetchMsg(executedUpTo + 1, transferTarget));
        armFetchTimer(FETCH_RETRY_MS);
    }

    private void armFetchTimer(long delayMs) {
        if (fetchTimer != null) fetchTimer.cancel(false);
        fetchTimer = runtime.setUpTimer(new FetchTimer(transferTarget), delayMs, TimeUnit.MILLISECONDS);
    }

    private void stopTransfer() {
        transferTarget = 0;
        if (fetchTimer != null) { fetchTimer.cancel(false); fetchTimer = null; }
    }

    // TIMERS

    @UponBabelEvent
    public void uponFetchTimer(FetchTimer t) {
        if (transferTarget == 0 || t.transferTarget() != transferTarget) return;   // stale
        fetchTimer = null;
        requestFetch();
    }
    
    /**
     * Handler of the timer that fires when the primary is suspected to be faulty.
     */
    @UponBabelEvent
    public void uponSuspectPrimary(SuspectPrimary suspect) throws GeneralSecurityException {
        if (suspect.view() == currentView && !inViewChange)
            startViewChange(currentView + 1);
    }

    // view change

    private static byte[] viewChangeBytes(long newView, long stableSeqn, List<PreparedProof> proofs) {
        StringBuilder sb = new StringBuilder("ViewChange|").append(newView).append('|').append(stableSeqn);
        for (PreparedProof p : proofs) {
            ProofEntry entry = p.preprepare();
            sb.append('|').append(entry.view()).append(':').append(entry.seqn()).append(':').append(entry.digest());
        }

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Returns the set P of the paper, containing all proofs of requests that reached the prepared state at this replica.
     * If there are many vouching for the same sequence number, only ones from the latest view are relevant.
     */
    private List<PreparedProof> getSetP() {
        Map<Long, PreparedProof> best = new TreeMap<>();
        for (PBFTState state : instances.values()) {
            if (!state.isPrepared() || state.getProof().preprepare().seqn() <= lastStableSeqn)
                continue;
            PreparedProof proof = state.getProof();
            best.merge(proof.preprepare().seqn(), proof,
                    (a, b) -> a.preprepare().view() >= b.preprepare().view() ? a : b);
        }

        return List.copyOf(best.values());
    }

    private void startViewChange(long newView) throws GeneralSecurityException {
        inViewChange = true;
        pendingView = newView;

        // P: for each seqn above the stable checkpoint, the prepared proof of the highest view
        List<PreparedProof> setP = getSetP();

        byte[] sig = Crypto.sign(viewChangeBytes(newView, lastStableSeqn, setP), runtime.getKeys().getPrivate());
        ViewChangeMsg vc = new ViewChangeMsg(newView, lastStableSeqn, stableProof, setP, runtime.getMyself(), sig);
        LOGGER.info("Starting view change to view {} with {} prepared proofs", newView, setP.size());

        viewChanges.computeIfAbsent(newView, k -> new HashMap<>()).put(runtime.getMyself(), vc);
        sendToOthers(vc);

        tryNewView(newView);
    }

    private boolean validateProof(PreparedProof proof) {
        ProofEntry pp = proof.preprepare();
        if (!neighbors.contains(pp.sender()) || !pp.sender().equals(expectedPrimary(pp.view())))
            return false;

        if (!pp.digest().equals(PBFTState.batchDigest(proof.batch())))
            return false;

        if (!Crypto.verify(prePrepareBytes(pp.view(), pp.seqn(), pp.digest()),
                pp.signature(),
                ((PublicKeyBabelNode) pp.sender()).identity()))
            return false;

        Set<BabelNode> signers = new HashSet<>();
        for (ProofEntry p : proof.prepares()) {
            if (p.view() != pp.view() || p.seqn() != pp.seqn() || !p.digest().equals(pp.digest()))
                return false;
            // only backups, each counted once (the primary's vote is its PrePrepare)
            if (p.sender().equals(pp.sender()) || !signers.add(p.sender()))
                return false;
            if (!Crypto.verify(prepareBytes(p.view(), p.seqn(), p.digest()),
                    p.signature(),
                    ((PublicKeyBabelNode) p.sender()).identity()))
                return false;
        }

        return signers.size() >= prepareQuorum - 1; // 2f prepares + the PrePrepare
    }

    private boolean validateViewChange(ViewChangeMsg vc) {
        if (!Crypto.verify(viewChangeBytes(vc.newView(), vc.stableSeqn(), vc.proofs()), vc.sig(),
                ((PublicKeyBabelNode) vc.sender()).identity()))
            return false;
        if (!validateStable(vc.stableSeqn(), vc.checkpointProof()))
            return false;

        Set<Long> seqns = new HashSet<>();
        for (PreparedProof proof : vc.proofs()) {
            long s = proof.preprepare().seqn();
            // one proof per seqn, and it must come from a view older than the one being installed
            if (s <= vc.stableSeqn() || !seqns.add(s) || proof.preprepare().view() >= vc.newView())
                return false;
            if (!validateProof(proof))
                return false;
        }
        return true;
    }

    private void adoptStableCheckpoint(Collection<ViewChangeMsg> msgs) {
        ViewChangeMsg best = null;
        for (ViewChangeMsg vc : msgs)
            if (vc.stableSeqn() > lastStableSeqn && (best == null || vc.stableSeqn() > best.stableSeqn()))
                best = vc;
        if (best != null)
            makeStable(best.stableSeqn(), best.checkpointProof());
    }

    private boolean validateStable(long seqn, List<CheckpointMsg> proof) {
        if (seqn == 0) return proof.isEmpty();
        if (proof.isEmpty()) return false;

        String digest = proof.get(0).digest();
        Set<BabelNode> signers = new HashSet<>();
        for (CheckpointMsg cp : proof) {
            if (cp.seqn() != seqn || !cp.digest().equals(digest)
                    || !neighbors.contains(cp.sender()) || !signers.add(cp.sender()))
                return false;
            if (!Crypto.verify(checkpointBytes(seqn, digest), cp.sig(),
                    ((PublicKeyBabelNode) cp.sender()).identity()))
                return false;
        }
        return signers.size() >= checkpointQuorum;
    }

    @UponMessageIn
    public void uponViewChangeMsg(ViewChangeMsg msg, BabelNode sender) throws GeneralSecurityException {
        if (!sender.equals(msg.sender()) || msg.newView() <= currentView || !validateViewChange(msg))
            return;

        var votes = viewChanges.computeIfAbsent(msg.newView(), k -> new HashMap<>());
        votes.putIfAbsent(sender, msg);

        // amplification: f+1 replicas want a higher view -> join, so we don't get left behind
        if (!inViewChange && votes.size() >= amplificationQuorum)
            startViewChange(msg.newView());

        tryNewView(msg.newView());
    }

    /**
     * For every seqn in (minS, maxS], the batch of the prepared proof with the highest view among the
     * view-changes, or an empty batch (null request) if no replica proved anything for it, so there are no holes.
     * minS is the highest stable checkpoint among the view-changes. Deterministic, so backups can recompute and check it.
     */
    private SortedMap<Long, List<BabelMessage>> chooseBatches(Collection<ViewChangeMsg> msgs) {
        Map<Long, PreparedProof> best = new HashMap<>();
        long minS = 0L;
        for (ViewChangeMsg msg : msgs)
            minS = Math.max(minS, msg.stableSeqn());

        long maxS = minS;
        for (ViewChangeMsg msg : msgs) {
            for (PreparedProof proof : msg.proofs()) {
                long seqn = proof.preprepare().seqn();
                if (seqn <= minS) continue;
                maxS = Math.max(maxS, seqn);
                best.merge(seqn, proof, (a, b) -> a.preprepare().view() >= b.preprepare().view() ? a : b);
            }
        }

        SortedMap<Long, List<BabelMessage>> chosen = new TreeMap<>();
        for (long s = minS + 1; s <= maxS; s++) {
            PreparedProof proof = best.get(s);
            chosen.put(s, proof != null ? proof.batch() : List.of());
        }
        return chosen;
    }

    /** The set O of PrePrepares for {@code view}, signed by this replica (the new primary). */
    private List<PrePrepareMessage> computeO(long view, Collection<ViewChangeMsg> msgs) throws GeneralSecurityException {
        List<PrePrepareMessage> o = new ArrayList<>();
        for (var entry : chooseBatches(msgs).entrySet()) {
            long seqn = entry.getKey();
            List<BabelMessage> batch = entry.getValue();
            String digest = PBFTState.batchDigest(batch);
            byte[] sig = Crypto.sign(prePrepareBytes(view, seqn, digest), runtime.getKeys().getPrivate());
            o.add(new PrePrepareMessage(batch, view, seqn, digest, sig));
        }
        return o;
    }

    private void tryNewView(long newView) throws GeneralSecurityException {
        if (newView <= currentView || !inViewChange || pendingView != newView)
            return;

        var votes = viewChanges.get(newView);
        if (votes == null || votes.size() < newViewQuorum) return;
        if (!expectedPrimary(newView).equals(runtime.getMyself())) return;

        List<ViewChangeMsg> proofs = List.copyOf(votes.values());
        List<PrePrepareMessage> o = computeO(newView, proofs);

        NewViewMsg nv = new NewViewMsg(newView, proofs, o);
        LOGGER.info("Sending NewView for view {} with {} PrePrepares in O", newView, o.size());
        sendToOthers(nv);
        adoptStableCheckpoint(proofs);
        installView(newView, o);
    }

    @UponMessageIn
    public void uponNewViewMsg(NewViewMsg msg, BabelNode sender) throws GeneralSecurityException {
        long view = msg.view();
        if (view <= currentView || !sender.equals(expectedPrimary(view)))
            return;

        // 2f+1 valid ViewChange messages for this view from distinct replicas
        Set<BabelNode> signers = new HashSet<>();
        for (ViewChangeMsg vc : msg.viewChanges()) {
            if (vc.newView() != view || !validateViewChange(vc) || !signers.add(vc.sender())) {
                LOGGER.warn("Invalid NewView for view {} from {}", view, sender);
                return;
            }
        }
        if (signers.size() < newViewQuorum)
            return;

        // O must be exactly what we compute from those view-changes
        var expected = chooseBatches(msg.viewChanges());
        if (msg.o().size() != expected.size())
            return;

        int i = 0;
        for (var entry : expected.entrySet()) {
            PrePrepareMessage pp = msg.o().get(i++);
            String digest = PBFTState.batchDigest(entry.getValue());
            if (pp.view() != view || pp.seqn() != entry.getKey()
                    || !pp.hash().equals(digest) || !digest.equals(PBFTState.batchDigest(pp.ms()))
                    || !Crypto.verify(prePrepareBytes(view, pp.seqn(), digest), pp.sig(),
                            ((PublicKeyBabelNode) sender).identity())) {
                LOGGER.warn("NewView for view {} has a wrong O at seqn {}", view, entry.getKey());
                return;
            }
        }

        adoptStableCheckpoint(msg.viewChanges());
        installView(view, msg.o());
    }

    /**
     * Moves to {@code view} and processes O as if each PrePrepare had been received normally: backups send their
     * Prepare, so the normal prepare/commit phases run again for them (Decided is deduplicated by executedUpTo/decidedDigests).
     */
    private void installView(long view, List<PrePrepareMessage> o) throws GeneralSecurityException {
        BabelNode primary = expectedPrimary(view);
        boolean iAmPrimary = primary.equals(runtime.getMyself());

        currentView = view;
        inViewChange = false;
        deferredBatches.clear(); // SMR re-proposes what is still unexecuted upon ViewChanged
        viewChanges.keySet().removeIf(v -> v <= view);

        long maxS = lastStableSeqn;
        for (PrePrepareMessage pp : o) {
            maxS = Math.max(maxS, pp.seqn());
            PBFTState state = stateFor(view, pp.seqn());
            state.setPrePrepared(pp, primary);

            if (iAmPrimary)
                continue;

            byte[] sig = Crypto.sign(prepareBytes(view, pp.seqn(), pp.hash()), runtime.getKeys().getPrivate());
            PrepareMessage prepare = new PrepareMessage(view, pp.seqn(), pp.hash(), sig);
            state.registerPrepare(runtime.getMyself(), prepare);
            sendToOthers(prepare);
            tryPrepared(state, view, pp.seqn());   // prepares of this view may have been buffered
        }
        currentSeqn = Math.max(currentSeqn, maxS); // the new primary continues after O

        LOGGER.info("Installed view {} (primary {})", view, primary);
        runtime.deliverEvent(new ViewChanged(view, primary));
    }
}
