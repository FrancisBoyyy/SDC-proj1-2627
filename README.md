# SDC 2026/27 - Project 1: Replicated Payment Service

This is the implementation for Project 1 of Reliable Distributed Systems
(Sistemas Distribuídos Confiáveis); the project statement remains the authority
for the requirements.

This repository contains three selectable replication variants: Byzantine
Reliable Broadcast (BRB), PBFT state machine replication, and a proof-of-work
blockchain. The payment service and client are shared by all variants.

The project began from version 1.1 of the teacher-provided starter code. See
[Babel 2 version note](#babel-2-version-note) for the original framework update.

## Requirements

* Java 25 or later (the code uses compact source files, e.g. `Main.java`)
* Babel 2 (babel2-core 2.1.1 and babel2-ip-core 2.3.0), downloaded
  automatically by Maven
* Maven 3.9 or later

## Building and running

```bash
mvn clean package
scripts/run-local.sh 4 num_clients=10 run_time=20000
```

The first command compiles the project, runs the unit tests, and creates
`target/project1-1.0.jar`. The second command starts four local replicas and an
automated client. If `replication` is not specified, the replicas run the
blockchain variant. The included `crypto-material/` supports four replicas and
clients `client1` through `client50`; addresses are configured in `babel.conf`
and use localhost ports 9001--9004 (replicas) and 8001--8004 (clients).

`run-local.sh N` starts replicas `node1` ... `nodeN` (they must exist in the
trust store and in `babel.conf`), runs the automated client (remaining
arguments are client properties), stops everything when the client ends, and
prints each replica's final state digest. Matching digests are expected when
the selected protocol and workload converge; a mismatch may be a protocol
result to investigate. The script waits until replica states stop changing, up
to `MAX_WAIT` seconds (60 by default; override it as an environment variable).
`REPLICA_OPTS` supplies properties to every replica. Logs are written to
`logs/` and replaced on each run. Examples:

```bash
# Blockchain (also the default)
scripts/run-local.sh 4 replication=blockchain num_clients=10 run_time=20000

# BRB consensus-free variant
REPLICA_OPTS="replication=brb" \
  scripts/run-local.sh 4 num_clients=10 run_time=20000 label=brb-smoke

# PBFT using the local evaluation parameters
REPLICA_OPTS="replication=pbft batch_size=10 batch_timeout=50 suspect_timeout=5000 checkpoint_interval=100 log_size=200 retain=10000 fetch_range_cap=10000" \
  scripts/run-local.sh 4 num_clients=10 run_time=20000 label=pbft-smoke

# Hard-kill node3 ten seconds after the client starts
scripts/run-local-2.sh --kill 3:10 4 num_clients=10 run_time=20000
```

`run-local-2.sh` accepts repeated `--kill <node>:<seconds>` options. It uses
`KILL` by default; set `KILL_SIGNAL=TERM` for graceful termination. Both
scripts observe replica state briefly after the client exits before printing
digests. BRB does not totally order concurrent transfers from shared accounts,
and blockchain finality is probabilistic, so equal digests are not guaranteed
for every workload or schedule.

You can also start each process by hand:

```bash
JAVA_OPTS="-Dio.netty.transport.noNative=true"
java $JAVA_OPTS -jar target/project1-1.0.jar crypto_name=node1          # one per replica
java $JAVA_OPTS -cp target/project1-1.0.jar ClientMain                  # automated client
java $JAVA_OPTS -cp target/project1-1.0.jar ClientMain client_mode=interactive
```

The local scripts run all replicas on one host. For a multi-host run, put the
jar, `babel.conf`, and the configured key material on each host; set every
`nodeX.replica_address` and `nodeX.client_address` to reachable host addresses
in the shared configuration; then start exactly one replica per host with its
own `crypto_name=nodeX`. Start a client process from any host that can reach
the client ports. Pass the same `replication` and protocol parameters to every
replica.

Any property of `babel.conf` can be overridden in the command line of the
process that uses it. The jar includes a default logging configuration; to see
the debug messages of Babel 2, add `-Dlog4j2.configurationFile=./log4j2-debug.xml`.

**macOS:** always use `-Dio.netty.transport.noNative=true`. Netty's native
transport for macOS (kqueue) misbehaves in some machines (e.g., it does not
support multicast); the portable NIO transport works everywhere.

## Protocol selection and parameters

Set `replication` on every replica. Supported values are `blockchain` (default),
`brb` or `consensus-free`, and `pbft`. In the local runner, set this in
`REPLICA_OPTS`; when starting replicas manually, pass it on each replica's
command line. All replicas must use the same protocol and matching parameter
values. Membership comes from the replica certificates in `trust_store`;
four replicas give the configured fault bound `f = 1`, and seven give `f = 2`.

Properties can be set in `babel.conf` or overridden on the command line. Time
values are in milliseconds.

| Replica property | Default | Meaning |
|---|---:|---|
| `replication` | `blockchain` | `blockchain`, `brb` / `consensus-free`, or `pbft` |
| `threads` | `4` | Netty event-loop threads shared by replica protocols |
| `event_queue_size` | `65536` | Capacity of each protocol event queue |
| `digest_interval` | `5000` | Period between replica state-digest log lines |
| `audit_enabled` | `false` | Detailed per-transfer audit logging for safety experiments; leave off for ordinary runs |
| `reconnect_interval` | `1000` | Membership reconnect interval |

Blockchain properties (`replication=blockchain`):

| Property | Default | Meaning |
|---|---:|---|
| `blockchain_difficulty_bits` | `16` | Leading zero bits required in a mined block hash |
| `blockchain_block_size` | `100` | Maximum transfers per block |
| `blockchain_confirmations` | `2` | Descendant blocks required before transfers are handed to the payment application; executed transfers are not rolled back |
| `blockchain_max_orphans` | `1000` | Maximum retained blocks whose parent has not arrived |
| `blockchain_max_pending_transfers` | `10000` | Maximum transfers held in the local mining pool |

PBFT properties (`replication=pbft`):

| Property | Default | Meaning |
|---|---:|---|
| `batch_size` | `10` | Maximum transfers proposed in one batch |
| `batch_timeout` | `2000` | Delay before proposing a partial batch |
| `suspect_timeout` | `2000` | Delay before suspecting an unresponsive primary |
| `checkpoint_interval` | `100` | Sequence interval for checkpoints |
| `log_size` | `200` | Active protocol sequence window |
| `retain` | `10000` | Decided sequence entries retained for recovery |
| `fetch_range_cap` | `10000` | Maximum entries requested in one recovery fetch |
| `fetch_grace_time` | `300` | Grace period before recovery fetch begins |
| `fetch_retry_time` | `300` | Delay between recovery fetch attempts |

Client properties (passed after the replica count to `run-local.sh`):

| Property | Default | Meaning |
|---|---:|---|
| `first_client` / `num_clients` | `1` / `10` | Logical client identities emulated by the process |
| `client_mode` / `client_threads` | `automated` / `2` | Automated load generator or interactive client; client event-loop threads |
| `transfer_percentage` | `80` | Percentage of operations that are transfers |
| `history_percentage` | `0` | Percentage of operations that read history; the remainder read balances |
| `shared_percentage` | `0` | Percentage of transfers attempted from shared accounts owned by the client |
| `max_amount` | `10` | Maximum transfer amount |
| `reply_quorum` | `f+1` | Matching replies from distinct replicas required to accept an operation |
| `operation_timeout` | `5000` | Client operation timeout |
| `prepare_time` / `run_time` / `cooldown_time` | `3000` / `30000` / `5000` | Preparation, measurement, and post-workload observation intervals |
| `results_file` / `label` | `results.csv` / `experiment` | Client CSV output path and run label |

`transfer_percentage + history_percentage` must not exceed 100; remaining
operations are balance reads. `shared_percentage` applies only to transfers.
For shared-account contention experiments, use separate client processes for
the different account owners.

Opt-in `*_byzantine_profile` properties are for controlled safety experiments,
not ordinary demonstrations. Supported values are `selective_send` for
`brb_byzantine_profile`; `equivocate_primary_once` (also `equivocate` or
`equivocate_primary`) for `pbft_byzantine_profile`; and `withhold_blocks`,
`broadcast_invalid_block`, or `selective_send` for
`blockchain_byzantine_profile`. The default is `none`. Do not set a fault profile
globally through `REPLICA_OPTS` unless the experiment intentionally makes every
replica with that profile faulty; assign it only to the intended replica when
launching processes individually.

## Cryptographic material and configuration

`crypto-material/` has keys for 4 replicas (`node1..node4`) and 50 clients
(`client1..client50`), all with password `password`. To generate a different
setting (e.g., 7 replicas and 200 clients):

```bash
scripts/generate-keys.sh 7 200 crypto-material-7
```

The script removes existing `*.ks` and `*.cert` files in its output directory,
so use a new output folder to preserve the supplied keys. To use the generated
set, update `key_store_folder`, `trust_store`, `clients_key_store`, and
`clients_trust_store` in `babel.conf`, and add `node5`...`node7` replica and
client addresses. The configured client identity range must include every
identity used by the workload. Membership is static and comes from certificates
in the replica trust store. The shared-account syntax is
`account:owner1,owner2;account2:owner3,owner4`.

## Structure

| Package / file | Contents | Should you change it? |
|---|---|---|
| `Main.java` | Replica: selects and registers BRB, PBFT, or Blockchain | Usually no |
| `ClientMain.java`, `client` | Automated and interactive clients | Only if needed |
| `app` | `PaymentService` (application protocol) | Avoid it |
| `app.ledger` | `Ledger`: deterministic state of the service | No |
| `app.messages` | Client requests/replies and `Transfer` | No |
| `replication` | BRB-backed replication and PBFT state-machine adapter | Protocol implementation |
| `blockchain` | Proof-of-work blocks, miner, transaction pool, fork state and replication | Protocol implementation |
| `replication.events` | `SubmitTransaction` (request), `ExecuteTransaction` (indication) | No |
| `agreement.events` | `Propose`, `Decided`, `ViewChanged` (interface of PBFT) | Adjust if you must |
| `broadcast` | Byzantine reliable broadcast used by BRB and Blockchain | Protocol implementation |
| `membership` | Static membership (connections among replicas) | Only if needed |
| `utils` | Keys, signatures, configuration | Add what you need |

### Interfaces between layers

* Application -> Replication: `SubmitTransaction(transfer)` (request);
  Replication -> Application: `ExecuteTransaction(transfer)` (indication, one
  transfer at a time, in the order in which they must be executed).
* Replication -> Broadcast: `broadcast(runtime, message)` (brbBroadcast);
  Broadcast -> Replication: `@UponBroadcastDeliver` (brbDeliver). The BRB
  implementation is a `BroadcastProviderBabelProtocol`.
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

## Known behavior and limitations

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

`mvn test` runs unit tests for the ledger and audit journal, message
serialization, BRB and its replication adapter, blockchain mining/state/pool
and configuration, client reply quorums and metrics, and utilities. PBFT's
fault and recovery behavior is covered by run-time experiments rather than a
dedicated PBFT unit-test class.

## Babel 2 version note

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

The table above records only the original starter-code change from version 1.0
to version 1.1. This repository has since added the BRB, PBFT, and Blockchain
implementations, related tests, and experiment support described in this README.

If you already started from version 1.0, you do not need to start over: in
your `pom.xml`, change the version of `babel2-ip-core` and of
`babel2-ip-compiler` (in the annotation processor configuration) from 2.1.0 to
2.3.0, and run `mvn clean package`. Then you may set `threads` in `babel.conf`
to more than 1 (and drop the warning in `Main.java`); `threads=1` keeps
working. Your protocols do not need any change, since the API of Babel is the
same.
