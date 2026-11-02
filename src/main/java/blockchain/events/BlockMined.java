package blockchain.events;

import blockchain.Block;
import pt.unl.fct.di.novasys.babel2.events.ProtocolEvent;

public record BlockMined(long generation, Block block) implements ProtocolEvent {
}