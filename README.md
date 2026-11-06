# SDC 2026/27 - Project 1: Replicated Payment Service (base code)

This is the base code for the first project of Reliable Distributed Systems
(Sistemas Distribuídos Confiáveis). Read the project statement first.

It contains a complete (and correct) application layer, a client (load
generator and interactive console), and the infrastructure to run the system.
The replication layer is a **placeholder that is evidently incorrect**:
replacing it is your job.

Version 1.1 (11 October 2026). See [Changes](#changes) at the end of this file
for what changed since version 1.0.

## Requirements

* Java 25 or later (the code uses compact source files, e.g. `Main.java`)
* Babel 2 (babel2-core 2.1.1 and babel2-ip-core 2.3.0), downloaded
  automatically by Maven
* Maven 3.9 or later

## Building and running

```bash
mvn clean package            # builds target/project1-1.0.jar (and runs the unit tests)
scripts/run-local.sh 4 num_clients=10 run_time=20000
```

`run-local.sh N` starts replicas `node1` ... `nodeN` (they must exist in the
trust store and in `babel.conf`), runs the automated client (the remaining
arguments are properties for the client), stops everything when the client ends,
and prints the final state digest of each replica (correct replicas should end
with the same digest; the script waits until the state of the replicas stops
changing, up to `MAX_WAIT` seconds, 60 by default). Properties for the replicas
can be given in `REPLICA_OPTS`, e.g. `REPLICA_OPTS="digest_interval=2000"`.
Logs are written to `logs/`. You can also start each process by hand:

```bash
JAVA_OPTS="-Dio.netty.transport.noNative=true"
java $JAVA_OPTS -jar target/project1-1.0.jar crypto_name=node1          # one per replica
java $JAVA_OPTS -cp target/project1-1.0.jar ClientMain                  # automated client
java $JAVA_OPTS -cp target/project1-1.0.jar ClientMain client_mode=interactive
```

Any property of `babel.conf` can be overridden in the command line of the
process that uses it. The jar includes a default logging configuration; to see
the debug messages of Babel 2, add `-Dlog4j2.configurationFile=./log4j2-debug.xml`.

**macOS:** always use `-Dio.netty.transport.noNative=true`. Netty's native
transport for macOS (kqueue) misbehaves in some machines (e.g., it does not
support multicast); the portable NIO transport works everywhere.

## Cryptographic material and configuration

`crypto-material/` has keys for 4 replicas (`node1..node4`) and 50 clients
(`client1..client50`), all with password `password`. To generate a different
setting (e.g., 7 replicas and 200 clients):

```bash
scripts/generate-keys.sh 7 200 crypto-material
```

and add `nodeX.replica_address` / `nodeX.client_address` to `babel.conf` for
every replica. The membership is static: it is given by the certificates in
`crypto-material/truststore.ks`. Every client owns an account with its own
name; shared accounts (several owners) are defined by `shared_accounts`.

## Structure

| Package / file | Contents | Should you change it? |
|---|---|---|
| `Main.java` | Replica: creates channels and registers protocols | Yes (register your protocols) |
| `ClientMain.java`, `client` | Automated and interactive clients | Only if needed |
| `app` | `PaymentService` (application protocol) | Avoid it |
| `app.ledger` | `Ledger`: deterministic state of the service | No |
| `app.messages` | Client requests/replies and `Transfer` | No |
| `replication` | `ReplicationProtocolPlaceholder` (incorrect!) | Replace it |
| `replication.events` | `SubmitTransaction` (request), `ExecuteTransaction` (indication) | No |
| `agreement.events` | `Propose`, `Decided`, `ViewChanged` (interface of PBFT) | Adjust if you must |
| `broadcast` | Crash-fault (eager) reliable broadcast | Replace by BRB |
| `membership` | Static membership (connections among replicas) | Only if needed |
| `utils` | Keys, signatures, configuration | Add what you need |

### Interfaces between layers

* Application -> Replication: `SubmitTransaction(transfer)` (request);
  Replication -> Application: `ExecuteTransaction(transfer)` (indication, one
  transfer at a time, in the order in which they must be executed).
* Replication -> Broadcast: `broadcast(runtime, message)` (brbBroadcast);
  Broadcast -> Replication: `@UponBroadcastDeliver` (brbDeliver). Implement
  your BRB as a `BroadcastProviderBabelProtocol`, exactly like the provided
  crash-fault broadcast.
* State Machine Replication -> PBFT: `Propose(value)` and `SuspectPrimary(view)`
  (requests); PBFT -> State Machine Replication: `Decided(sqn, value)` and
  `ViewChanged(view, primary)` (indications).

`PaymentService` submits each transfer to the replication protocol only once
(even if several clients, or retransmissions, deliver it again). If your
replication protocol loses a transfer (e.g., during a view change), it will never
be executed: making sure that submitted transfers are eventually executed is the
job of the replication protocol.

Requests and indications are Babel events (`ProtocolEvent` records): trigger
them with `runtime.deliverEvent(event)` and handle them with `@UponBabelEvent`.

## Things you must know about Babel 2

Babel 2 is developed by Rafael Matos, as the evolution of Babel. It is still
under active development: if you find a bug, report it to him
(rd.matos@campus.fct.unl.pt, subject "Babel 2 Bug Report"), as thoroughly as
you can, attaching the `output.log` produced with `-Dlog4j2.configurationFile=./log4j2-debug.xml`.


* **Exceptions in handlers.** If an exception escapes a handler, Babel 2 logs
  it and discards the rest of the handler (the protocol continues with the next
  event). Your protocol might then be left in an inconsistent state: never let
  an exception escape, in particular when handling messages from (potentially
  Byzantine) processes.
* **Messages are delivered by type, not by channel.** A message received on
  any channel is delivered to every protocol with an `@UponMessageIn` handler
  for its type. A client can therefore send your BRB or PBFT messages to the
  client port of a replica: always check that the sender is a replica (see the
  provided broadcast protocol).
* **Authenticated links.** Channels verify the identity of the other side when
  connections are established, so the `sender` of every message is
  authenticated (authenticated perfect point-to-point links). Whenever a third
  party must verify that a process said something (e.g., certificates in PBFT,
  or messages relayed by others), sign the content explicitly (see
  `utils.Crypto` and `Transfer`).
* **Full event queues drop events.** Each protocol has a bounded event queue
  (`event_queue_size`). With the netty executor used by the base code
  (`NettyExecutorProtocolHolderFactory`), Babel 2 silently drops new events
  (including received messages) when the queue is full. Keep handlers fast, and
  keep the queue large.
* **Signed messages.** The signed message extension (`SignedBabelMessage`,
  `@UponSignedMessageIn`) delivers every signed message to every protocol that
  uses it; protocols without a handler for that type ignore it. Signatures made
  by the channel cannot be presented to third parties after the message is
  received: whenever that is needed, sign the content explicitly.

## Known limitations of the base code

* The ledger accepts transfers with any (future) sequence number, which stay
  pending until all previous transfers of the account are executed; a client can
  therefore make replicas keep a large number of pending transfers. Consider
  whether (and how) your solution should mitigate this kind of attack.
* Conflicting transfers of a shared account only make replicas diverge if they
  reach replicas in different orders. Clients emulated by the same process send
  over the same connections, so their transfers tend to arrive everywhere in the
  same order: to study divergence, run the owners of a shared account in
  different client processes, e.g. `first_client=1 num_clients=1` and
  `first_client=2 num_clients=1`, both with `shared_percentage=100`.
* The number of operations with divergent replies reported by the client is a
  lower bound: replies that arrive after a result was accepted are not inspected.

## Tests

`mvn test` runs the unit tests of the ledger (including a test that shows that
single-owner accounts converge without total order, while shared accounts may
not), of the message serializers, and of the client quorum logic.

## Changes

### Version 1.1 (11 October 2026)

Version 1.0 used Babel 2.1 (babel2-ip-core 2.1.0), whose TCP channel corrupts
connections among replicas under load when a replica uses more than one netty
thread ("Invalid Magic Number" in the logs; the connection is closed and the
messages in transit are lost). Version 1.0 therefore forced a single thread.
Two bugs in Babel caused this, and both are fixed in babel2-ip-core 2.3.0:

* a message could be written in the middle of another one on the same
  connection (fixed in 2.2.0);
* a `BabelNode` (for instance, the `sender` or a node carried inside a message)
  could be serialized with a wrong number of addresses while connections were
  being established (fixed in 2.3.0).

What changed in the base code:

| File | Version 1.0 | Version 1.1 |
|---|---|---|
| `pom.xml` | `babel2-ip-core` and `babel2-ip-compiler` 2.1.0 | 2.3.0 (`babel2-core` stays at 2.1.1) |
| `babel.conf` | `threads=1`, with a warning | `threads=4` |
| `Main.java` | default of 1 thread, and a warning printed when `threads` was not 1 | default of 4 threads, no warning |
| `README.md` | "One netty thread per replica" item in "Things you must know about Babel 2" | item removed (no longer true); this section added |

Nothing else changed: the application layer, the client, the protocol
interfaces, the scripts, the tests and the cryptographic material are the same
as in version 1.0.

If you already started from version 1.0, you do not need to start over: in
your `pom.xml`, change the version of `babel2-ip-core` and of
`babel2-ip-compiler` (in the annotation processor configuration) from 2.1.0 to
2.3.0, and run `mvn clean package`. Then you may set `threads` in `babel.conf`
to more than 1 (and drop the warning in `Main.java`); `threads=1` keeps
working. Your protocols do not need any change, since the API of Babel is the
same.
