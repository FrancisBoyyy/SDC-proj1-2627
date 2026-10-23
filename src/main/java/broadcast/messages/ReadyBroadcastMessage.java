package broadcast.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.SignedBabelMessage;

// Messages only need to implement the BabelMessage interface. Additionally, we can also annotate with the BabelIPMessage
// annotation to generate the serializer for us. If you use the annotation, the message MUST be a record.
// You can write your own serializer if you want to (although I do not recomend). Compile
// the project at least once and look for the target/generated-sources directory to see an example for the messages in this project.
// If an element can be null, you should annotate it with the @Nullable annotation from the jspecify package.

// For the automatic serializers, messages can have any of the primitive types and arrays of them, Strings, UUID objects, BabelNode interface,
// any of the following java collection framework interfaces (List, Map, SortedMap, Set, SortedSet)
// (deserialized messages from the network will be constructued with immutable versions of the collections),
// BabelMessage with a registered serializer, or any other class that has a registered serializer. Do note that you
// MUST write the serializers for other classes using the
// BabelNetworkSerializer interface and annotate it with @AutoService(BabelNetworkSerializer.class)
@BabelIPMessage(id = 4)
public record ReadyBroadcastMessage(SignedBabelMessage<BroadcastMessage> message) implements BabelMessage {

}
