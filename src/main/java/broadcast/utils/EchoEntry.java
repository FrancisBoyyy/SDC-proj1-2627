package broadcast.utils;

import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;

public record EchoEntry(BabelNode p, BabelMessage m) {

    /**
     * Returns true if this entry has the same p as the other.
     *
     * @param other
     * @return
     */
    public boolean compare(EchoEntry other) {
        return p.equals(other.p);
    }
}

