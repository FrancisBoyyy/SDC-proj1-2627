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
import utils.Crypto;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

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
    private final Set<Long> decidedSeqns; // to not emit Decided twice!

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
        this.decidedSeqns = new HashSet<>();
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
        List<BabelMessage> batch = propose.batch();

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
    public void uponCommitMsg(CommitMessage msg, BabelNode sender) {
        if (inViewChange)
            return;

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

    private void tryCommitted(PBFTState state, long seqn) {
        if (state.getCommitSize() < commitQuorum)
            return;

        state.setCommited();

        if (!decidedSeqns.add(seqn))
            return; // already decided in an earlier view (re-agreed through O)

        Decided decided = new Decided(seqn, state.getBatch());
        LOGGER.info("Commits quorum reached -> Decide {} sent to SMR", decided);
        runtime.deliverEvent(decided);
    }

    // TIMERS

    /**
     * Handler of the timer that fires when the primary is suspected to be faulty.
     */
    @UponBabelEvent
    public void uponSuspectPrimary(SuspectPrimary suspect) throws GeneralSecurityException {
        if (suspect.view() == currentView && !inViewChange)
            startViewChange(currentView + 1);
    }

    // view change

    private static byte[] viewChangeBytes(long newView, List<PreparedProof> proofs) {
        StringBuilder sb = new StringBuilder("ViewChange|").append(newView);
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
            if (!state.isPrepared())
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

        // P: for each seqn, the prepared proof of the highest view (nothing is garbage collected: no checkpoints)
        List<PreparedProof> setP = getSetP();

        byte[] sig = Crypto.sign(viewChangeBytes(newView, setP), runtime.getKeys().getPrivate());
        ViewChangeMsg vc = new ViewChangeMsg(newView, setP, runtime.getMyself(), sig);
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
        if (!Crypto.verify(viewChangeBytes(vc.newView(), vc.proofs()),
                vc.sig(),
                ((PublicKeyBabelNode) vc.sender()).identity()))
            return false;

        Set<Long> seqns = new HashSet<>();
        for (PreparedProof proof : vc.proofs()) {
            // one proof per seqn, and it must come from a view older than the one being installed
            if (!seqns.add(proof.preprepare().seqn()) || proof.preprepare().view() >= vc.newView())
                return false;
            if (!validateProof(proof))
                return false;
        }
        return true;
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
     * minS is 0 because there are no checkpoints. Deterministic, so backups can recompute and check it.
     */
    private SortedMap<Long, List<BabelMessage>> chooseBatches(Collection<ViewChangeMsg> msgs) {
        Map<Long, PreparedProof> best = new HashMap<>();
        long minS = 0L;
        long maxS = minS;
        for (ViewChangeMsg msg : msgs) {
            for (PreparedProof proof : msg.proofs()) {
                long seqn = proof.preprepare().seqn();
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

        installView(view, msg.o());
    }

    /**
     * Moves to {@code view} and processes O as if each PrePrepare had been received normally: backups send their
     * Prepare, so the normal prepare/commit phases run again for them (Decided is deduplicated by decidedSeqns).
     */
    private void installView(long view, List<PrePrepareMessage> o) throws GeneralSecurityException {
        BabelNode primary = expectedPrimary(view);
        boolean iAmPrimary = primary.equals(runtime.getMyself());

        currentView = view;
        inViewChange = false;
        viewChanges.keySet().removeIf(v -> v <= view);

        long maxS = 0L;
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
