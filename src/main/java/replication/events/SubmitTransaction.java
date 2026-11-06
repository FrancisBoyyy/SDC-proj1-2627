package replication.events;

import app.messages.Transfer;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

/**
 * Request (Application -> Replication Protocol): replicate the given transfer.
 *
 * <p>
 * The application issues this request when it receives a (valid) transfer
 * from a client. Notice that clients send their transfers to several replicas,
 * hence the same transfer might be submitted by several replicas (and a
 * Byzantine replica might submit whatever it wants).
 */
public record SubmitTransaction(Transfer transfer) implements ProtocolEvent {
}
