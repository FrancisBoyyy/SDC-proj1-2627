package broadcast.events;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SelectiveBroadcastRequestTest {

    @Test
    void rejectsNullPayload() {
        assertThrows(
                NullPointerException.class,
                () -> new SelectiveBroadcastRequest(null, 1));
    }

    @Test
    void rejectsNonPositiveRecipientCount() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SelectiveBroadcastRequest(new TestPayload(), 0));
    }

    private record TestPayload() implements pt.unl.fct.di.novasys.babel2.channels.BabelMessage {
    }
}
