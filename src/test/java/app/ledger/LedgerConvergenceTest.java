package app.ledger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.jupiter.api.Test;

import app.messages.Transfer;

/**
 * Checks the premise of the project: when every account has a single owner,
 * replicas that receive the same transfers in different orders (as with
 * Byzantine Reliable Broadcast, without total order) converge to the same
 * state. With shared accounts and concurrent owners, they may not.
 */
class LedgerConvergenceTest {

    private static final int CLIENTS = 6;
    private static final int REPLICAS = 4;

    @Test
    void singleOwnerAccountsConvergeWithoutTotalOrder() throws Exception {
        TestAccounts accounts = new TestAccounts(CLIENTS);
        Random random = new Random(42);
        for (int round = 0; round < 20; round++) {
            List<Transfer> workload = singleOwnerWorkload(accounts, random, 200);
            String expected = null;
            for (int r = 0; r < REPLICAS; r++) {
                List<Transfer> order = new ArrayList<>(workload);
                Collections.shuffle(order, random);
                Ledger replica = accounts.newLedger(50);
                order.forEach(replica::execute);
                assertEquals(replica.totalSupply(), replica.sumOfBalances());
                if (expected == null) {
                    expected = replica.stateDigest();
                }
                assertEquals(expected, replica.stateDigest(), "Replica " + r + " diverged in round " + round);
            }
        }
    }

    @Test
    void sharedAccountsMayDivergeWithoutTotalOrder() throws Exception {
        TestAccounts accounts = new TestAccounts(CLIENTS);
        // Both owners of the shared account use sequence number 0 concurrently
        Transfer byClient1 = accounts.transfer("client1", "shared", "client3", 10, 0);
        Transfer byClient2 = accounts.transfer("client2", "shared", "client4", 10, 0);

        Ledger replicaA = accounts.newLedger(100);
        replicaA.execute(byClient1);
        replicaA.execute(byClient2);

        Ledger replicaB = accounts.newLedger(100);
        replicaB.execute(byClient2);
        replicaB.execute(byClient1);

        assertTrue(!replicaA.stateDigest().equals(replicaB.stateDigest()),
                "Different orders of conflicting transfers must lead to different states");
    }

    @Test
    void sharedAccountsConvergeWithTotalOrder() throws Exception {
        TestAccounts accounts = new TestAccounts(CLIENTS);
        Random random = new Random(7);
        List<Transfer> workload = singleOwnerWorkload(accounts, random, 100);
        // Add conflicting transfers of the shared account
        for (int seq = 0; seq < 10; seq++) {
            workload.add(accounts.transfer("client1", "shared", "client3", 1 + seq, seq));
            workload.add(accounts.transfer("client2", "shared", "client4", 2 + seq, seq));
        }
        Collections.shuffle(workload, random);

        // Same (total) order at every replica: same state
        Ledger replicaA = accounts.newLedger(100);
        Ledger replicaB = accounts.newLedger(100);
        workload.forEach(replicaA::execute);
        workload.forEach(replicaB::execute);
        assertEquals(replicaA.stateDigest(), replicaB.stateDigest());
        assertEquals(10, replicaA.nextSeq("shared"));
    }

    /**
     * Each client issues transfers from its own account with consecutive sequence
     * numbers (some of them will have to wait for funds).
     */
    private static List<Transfer> singleOwnerWorkload(TestAccounts accounts, Random random, int size) {
        Map<String, Long> nextSeq = new HashMap<>();
        List<Transfer> workload = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String from = "client" + (1 + random.nextInt(CLIENTS));
            String to;
            do {
                to = "client" + (1 + random.nextInt(CLIENTS));
            } while (to.equals(from));
            long seq = nextSeq.merge(from, 1L, Long::sum) - 1;
            workload.add(accounts.transfer(from, to, 1 + random.nextInt(30), seq));
        }
        return workload;
    }
}
