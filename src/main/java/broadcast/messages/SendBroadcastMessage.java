package broadcast.messages;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

// Messages only need to implement the BabelMessage interface. Additionally, we can also annotate with the BabelIPMessage
// annotation to generate the serializer for us. If you use the annotation, the message MUST be a record.
// Compile the project at least once and look for the target/generated-sources directory to see the generated
// serializers. If an element can be null, you should annotate it with the @Nullable annotation from jspecify.

// For the automatic serializers, messages can have any of the primitive types and arrays of them, Strings, UUID
// objects, BabelNode interface, any of the following java collection framework interfaces (List, Map, SortedMap, Set,
// SortedSet) (deserialized messages from the network will be constructed with immutable versions of the collections),
// or BabelMessage with a registered serializer (such as the payload below).
@BabelIPMessage(id = 200)
public record SendBroadcastMessage(UUID id, BabelNode origin, BabelMessage payload) implements BabelMessage {
}
