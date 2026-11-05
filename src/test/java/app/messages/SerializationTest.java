package app.messages;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import broadcast.messages.SendBroadcastMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.DefaultNodeStore;
import pt.unl.fct.di.novasys.babel2.channels.PublicKeyBabelNode;
import pt.unl.fct.di.novasys.babel2.channels.serializers.BabelNetworkMessageSerializer;
import pt.unl.fct.di.novasys.babel2.channels.serializers.SerializerRegistry;
import utils.Crypto;

/**
 * Every message must survive a round trip through the serializers generated
 * by the Babel 2 annotation processor (and a transfer must keep a valid
 * signature after it).
 */
class SerializationTest {

    @Test
    void allMessagesRoundTrip() throws Exception {
        var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Transfer t = Transfer.create("client1", "client2", 42, 7, "client1", keys.getPrivate());
        UUID id = UUID.randomUUID();

        List<BabelMessage> messages = List.of(
                t,
                new TransferRequest(id, t),
                new TransferReply(id, "client1", 7, "EXECUTED", 12),
                new BalanceRequest(id, "shared1"),
                new BalanceReply(id, "shared1", true, 1000, 3),
                new HistoryRequest(id, "client1", 5),
                new HistoryReply(id, "client1", 0, 1, List.of(t)),
                new SendBroadcastMessage(id, new PublicKeyBabelNode(keys.getPublic()), new TransferRequest(id, t)));

        for (BabelMessage message : messages) {
            assertEquals(message, roundTrip(message), message.getClass().getSimpleName());
        }

        Transfer copy = (Transfer) roundTrip(t);
        assertEquals(t.id(), copy.id());
        assertEquals(true, Crypto.verify(copy.signedContent(), copy.signature(), keys.getPublic()));
    }

    @SuppressWarnings("unchecked")
    private static BabelMessage roundTrip(BabelMessage message) {
        var store = new DefaultNodeStore();
        BabelNetworkMessageSerializer<BabelMessage> serializer = (BabelNetworkMessageSerializer<BabelMessage>) SerializerRegistry
                .getMessageSerializerByClass(message.getClass());
        assertNotNull(serializer, "No serializer for " + message.getClass());
        ByteBuf buffer = Unpooled.buffer();
        serializer.serialize(message, buffer, store);
        BabelMessage result = serializer.deserialize(buffer, store);
        assertEquals(0, buffer.readableBytes(), "Serializer did not consume the whole buffer");
        buffer.release();
        return result;
    }
}
