package replication.events;

import app.messages.Transfer;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Indication (Replication Protocol -> Application): hand the given transfer to
 * the ledger.
 *
 * <p>
 * Transfers are indicated one at a time, in the order in which they must be
 * handed to the ledger. The guarantees on that order depend on the replication
 * protocol (no order across accounts with Byzantine Reliable Broadcast, the
 * same total order at all correct replicas with PBFT, and the order of the
 * (sufficiently deep) blocks of the chain with the blockchain).
 */
public record ExecuteTransaction(Transfer transfer) implements ProtocolEvent {
}
