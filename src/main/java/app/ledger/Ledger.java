package app.ledger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.PublicKey;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import app.messages.Transfer;
import utils.Crypto;

/**
 * The state of the payment service: a set of accounts and the ledger of
 * executed transfers.
 *
 * <p>
 * This class is deterministic: two replicas that hand the same transfers to
 * the ledger, in the same order, end up in exactly the same state.
 *
 * <p>
 * Execution rules for a transfer t from account A with sequence number s:
 * <ol>
 * <li>If t is invalid (see {@link #isValid(Transfer)}) it is discarded.</li>
 * <li>If a different transfer of A with sequence number s was already handed
 * to the ledger (executed or pending), t is a CONFLICT and is discarded.</li>
 * <li>Otherwise t becomes pending, and pending transfers of A are executed in
 * sequence number order (s = 0, 1, 2, ...) as long as A has enough funds.
 * A pending transfer that lacks funds is retried whenever A receives funds.</li>
 * </ol>
 *
 * <p>
 * Notice that if all transfers of an account are issued by a single (correct)
 * owner, the final state does not depend on the order in which transfers of
 * different accounts are handed to the ledger. That is no longer true when
 * several owners issue transfers of the same account concurrently (conflicts).
 */
public final class Ledger {

    private static final class Account {
        private final String name;
        private final Set<String> owners;
        private long balance;
        private long nextSeq;
        // Transfers that were handed to the ledger but not executed yet (by seq)
        private final TreeMap<Long, Transfer> pending;
        // Ids of the executed transfers of this account (index = seq)
        private final List<String> executedIds;
        // Executed transfers involving this account (outgoing and incoming)
        private final List<Transfer> history;
        // Hash chain of the ids of the executed transfers of this account (in seq order)
        private byte[] executedHash;

        private Account(String name, Set<String> owners, long balance) {
            this.name = name;
            this.owners = Set.copyOf(owners);
            this.balance = balance;
            this.nextSeq = 0;
            this.pending = new TreeMap<>();
            this.executedIds = new ArrayList<>();
            this.history = new ArrayList<>();
            this.executedHash = new byte[0];
        }
    }

    private final Map<String, Account> accounts;
    private final Map<String, PublicKey> ownerKeys;
    private final List<Transfer> ledger;
    private final long totalSupply;

    /**
     * @param accountOwners  the accounts in the initial state (account name -> owners)
     * @param ownerKeys      the public keys of the owners (owner name -> key)
     * @param initialBalance the initial balance of every account
     */
    public Ledger(Map<String, Set<String>> accountOwners, Map<String, PublicKey> ownerKeys, long initialBalance) {
        if (initialBalance < 0) {
            throw new IllegalArgumentException("Initial balance must not be negative");
        }
        this.accounts = new HashMap<>(accountOwners.size());
        for (var entry : accountOwners.entrySet()) {
            accounts.put(entry.getKey(), new Account(entry.getKey(), entry.getValue(), initialBalance));
        }
        this.ownerKeys = Map.copyOf(ownerKeys);
        this.ledger = new ArrayList<>();
        this.totalSupply = initialBalance * accounts.size();
    }

    /**
     * A transfer is valid if both accounts exist and are different, the amount is
     * positive, the sequence number is not negative, the signer is an owner of the
     * source account, and the signature is valid. Validity does not depend on the state of the ledger.
     */
    public boolean isValid(Transfer t) {
        Account from = accounts.get(t.from());
        if (from == null || !accounts.containsKey(t.to()) || t.from().equals(t.to())) {
            return false;
        }
        if (t.amount() <= 0 || t.seq() < 0 || !from.owners.contains(t.signer())) {
            return false;
        }
        PublicKey key = ownerKeys.get(t.signer());
        return key != null && Crypto.verify(t.signedContent(), t.signature(), key);
    }

    /**
     * Status of a transfer that has not been handed to the ledger by this caller
     * yet, without changing the state. Useful to answer a client immediately,
     * for instance when it retransmits a transfer that was already executed.
     *
     * @return EXECUTED (already executed), INVALID, CONFLICT, DUPLICATE (the same
     *         transfer is pending) or PENDING (meaning, here, that it was never
     *         handed to the ledger)
     */
    public TransferStatus statusOf(Transfer t) {
        if (!isValid(t)) {
            return TransferStatus.INVALID;
        }
        Account from = requireAccount(t.from());
        String id = t.id();
        if (t.seq() < from.nextSeq) {
            return from.executedIds.get((int) t.seq()).equals(id) ? TransferStatus.EXECUTED
                    : TransferStatus.CONFLICT;
        }
        Transfer pending = from.pending.get(t.seq());
        if (pending == null) {
            return TransferStatus.PENDING;
        }
        return pending.id().equals(id) ? TransferStatus.DUPLICATE : TransferStatus.CONFLICT;
    }

    /**
     * Hands a transfer to the ledger (this should be done by every replica, in
     * the order defined by the replication protocol).
     *
     * @return the outcome of the given transfer (EXECUTED, PENDING, INVALID,
     *         DUPLICATE if it was already handed to the ledger, pending or executed,
     *         or CONFLICT), followed by the outcome of every other transfer that was
     *         executed as a consequence (pending transfers that became executable).
     */
    public List<TransferOutcome> execute(Transfer t) {
        TransferStatus status = statusOf(t);
        if (status != TransferStatus.PENDING) {
            if (status == TransferStatus.EXECUTED) {
                // Re-delivery of an executed transfer: nothing changes
                status = TransferStatus.DUPLICATE;
            }
            return List.of(new TransferOutcome(t, status, -1));
        }

        Account from = requireAccount(t.from());
        from.pending.put(t.seq(), t);

        List<TransferOutcome> executed = new ArrayList<>();
        var toProcess = new ArrayDeque<Account>();
        toProcess.add(from);
        while (!toProcess.isEmpty()) {
            Account account = toProcess.poll();
            Transfer next;
            while ((next = account.pending.get(account.nextSeq)) != null && account.balance >= next.amount()) {
                account.pending.remove(account.nextSeq);
                Account to = requireAccount(next.to());
                account.balance -= next.amount();
                to.balance += next.amount();
                account.nextSeq++;
                String nextId = next.id();
                account.executedIds.add(nextId);
                account.executedHash = Crypto.digest(concat(account.executedHash,
                        nextId.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                account.history.add(next);
                to.history.add(next);
                ledger.add(next);
                executed.add(new TransferOutcome(next, TransferStatus.EXECUTED, ledger.size() - 1));
                // The destination might now have funds for one of its pending transfers
                if (!to.pending.isEmpty() && !toProcess.contains(to)) {
                    toProcess.add(to);
                }
            }
        }

        boolean selfExecuted = executed.stream().anyMatch(o -> o.transfer() == t);
        List<TransferOutcome> result = new ArrayList<>(executed.size() + 1);
        if (selfExecuted) {
            // Put the outcome of t first
            executed.stream().filter(o -> o.transfer() == t).forEach(result::add);
            executed.stream().filter(o -> o.transfer() != t).forEach(result::add);
        } else {
            result.add(new TransferOutcome(t, TransferStatus.PENDING, -1));
            result.addAll(executed);
        }
        return result;
    }

    public boolean exists(String account) {
        return accounts.containsKey(account);
    }

    public long balance(String account) {
        return requireAccount(account).balance;
    }

    public long nextSeq(String account) {
        return requireAccount(account).nextSeq;
    }

    public Set<String> owners(String account) {
        return requireAccount(account).owners;
    }

    public Set<String> accounts() {
        return Collections.unmodifiableSet(new TreeSet<>(accounts.keySet()));
    }

    public int historySize(String account) {
        return requireAccount(account).history.size();
    }

    /**
     * @return up to {@code max} executed transfers involving the account,
     *         starting at position {@code fromIndex} of its history.
     */
    public List<Transfer> history(String account, int fromIndex, int max) {
        var history = requireAccount(account).history;
        if (fromIndex < 0 || fromIndex >= history.size()) {
            return List.of();
        }
        return List.copyOf(history.subList(fromIndex, Math.min(history.size(), fromIndex + max)));
    }

    /**
     * @return the number of transfers executed so far
     */
    public int size() {
        return ledger.size();
    }

    public Transfer get(int index) {
        return ledger.get(index);
    }

    public int pendingTransfers() {
        int total = 0;
        for (var account : accounts.values()) {
            total += account.pending.size();
        }
        return total;
    }

    /**
     * The sum of all balances never changes (no money is created nor destroyed).
     */
    public long totalSupply() {
        return totalSupply;
    }

    public long sumOfBalances() {
        long sum = 0;
        for (var account : accounts.values()) {
            sum += account.balance;
        }
        return sum;
    }

    /**
     * A digest of the state of all accounts: balance, next sequence number, and
     * the (hash chain of the) transfers executed from each account. Two replicas
     * with the same digest executed the same transfers from every account (in the
     * order of their sequence numbers) and have the same balances, even if they
     * executed transfers of different accounts in a different order. Useful to
     * check whether correct replicas converged.
     */
    public String stateDigest() {
        try (var bytes = new ByteArrayOutputStream(); var out = new DataOutputStream(bytes)) {
            for (String name : new TreeSet<>(accounts.keySet())) {
                Account account = requireAccount(name);
                out.writeUTF(account.name);
                out.writeLong(account.balance);
                out.writeLong(account.nextSeq);
                out.write(account.executedHash);
            }
            out.flush();
            return Crypto.toHex(Crypto.digest(bytes.toByteArray()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] r = java.util.Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, r, a.length, b.length);
        return r;
    }

    private Account requireAccount(String account) {
        Account a = accounts.get(account);
        if (a == null) {
            throw new IllegalArgumentException("Unknown account " + account);
        }
        return a;
    }
}
