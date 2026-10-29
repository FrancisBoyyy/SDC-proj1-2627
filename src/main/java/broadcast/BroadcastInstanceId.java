package broadcast;

import java.util.UUID;

import pt.unl.fct.di.novasys.babel2.core.BabelNode;

record BroadcastInstanceId(BabelNode origin, UUID id) { }
