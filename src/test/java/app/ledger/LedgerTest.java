package app.ledger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import app.messages.Transfer;

class LedgerTest {

    private TestAccounts accounts;
    private Ledger ledger;

    @BeforeEach
    void setUp() throws Exception {
        accounts = new TestAccounts(4);
        ledger = accounts.newLedger(100);
    }

    @Test
    void genesisState() {
        assertEquals(5, ledger.accounts().size());
        assertEquals(100, ledger.balance("client1"));
        assertEquals(100, ledger.balance("shared"));
        assertEquals(0, ledger.nextSeq("client1"));
        assertEquals(500, ledger.totalSupply());
        assertEquals(500, ledger.sumOfBalances());
        assertEquals(0, ledger.size());
        assertFalse(ledger.exists("nobody"));
        assertThrows(IllegalArgumentException.class, () -> ledger.balance("nobody"));
    }

    @Test
    void executesValidTransfer() {
        Transfer t = accounts.transfer("client1", "client2", 30, 0);
        List<TransferOutcome> outcomes = ledger.execute(t);

        assertEquals(1, outcomes.size());
        assertEquals(TransferStatus.EXECUTED, outcomes.get(0).status());
        assertEquals(0, outcomes.get(0).ledgerIndex());
        assertEquals(70, ledger.balance("client1"));
        assertEquals(130, ledger.balance("client2"));
        assertEquals(1, ledger.nextSeq("client1"));
        assertEquals(0, ledger.nextSeq("client2"));
        assertEquals(List.of(t), ledger.history("client1", 0, 10));
        assertEquals(List.of(t), ledger.history("client2", 0, 10));
        assertEquals(500, ledger.sumOfBalances());
    }

    @Test
    void rejectsInvalidTransfers() {
        // Unknown accounts, same account, non positive amount
        assertInvalid(accounts.transfer("client1", "nobody", 1, 0));
        assertInvalid(accounts.transfer("client1", "client1", 1, 0));
        assertInvalid(accounts.transfer("client1", "client2", 0, 0));
        assertInvalid(accounts.transfer("client1", "client2", -5, 0));
        assertInvalid(accounts.transfer("client1", "client2", 1, -1));
        // Signed by someone who is not an owner of the account
        assertInvalid(accounts.transfer("client3", "client1", "client2", 1, 0));
        // Signer claims to be an owner, but the signature is from another key
        Transfer forged = accounts.transfer("client3", "client3", "client2", 1, 0);
        assertInvalid(new Transfer("client1", "client2", 1, 0, "client1", forged.signature()));
        // Tampered amount (signature no longer matches)
        Transfer original = accounts.transfer("client1", "client2", 1, 0);
        assertInvalid(new Transfer("client1", "client2", 99, 0, "client1", original.signature()));
        // Garbage signature
        assertInvalid(new Transfer("client1", "client2", 1, 0, "client1", new byte[] { 1, 2, 3 }));

        assertEquals(0, ledger.size());
        assertEquals(0, ledger.pendingTransfers());
        assertEquals(100, ledger.balance("client1"));
    }

    @Test
    void transfersOfAnAccountAreExecutedInSequenceOrder() {
        Transfer t0 = accounts.transfer("client1", "client2", 1, 0);
        Transfer t1 = accounts.transfer("client1", "client3", 2, 1);
        Transfer t2 = accounts.transfer("client1", "client4", 3, 2);

        assertEquals(TransferStatus.PENDING, ledger.execute(t2).get(0).status());
        assertEquals(TransferStatus.PENDING, ledger.execute(t1).get(0).status());
        assertEquals(2, ledger.pendingTransfers());
        assertEquals(0, ledger.size());

        List<TransferOutcome> outcomes = ledger.execute(t0);
        assertEquals(3, outcomes.size());
        assertEquals(t0, outcomes.get(0).transfer());
        assertEquals(t1, outcomes.get(1).transfer());
        assertEquals(t2, outcomes.get(2).transfer());
        outcomes.forEach(o -> assertEquals(TransferStatus.EXECUTED, o.status()));
        assertEquals(3, ledger.nextSeq("client1"));
        assertEquals(94, ledger.balance("client1"));
        assertEquals(0, ledger.pendingTransfers());
    }

    @Test
    void transferWithoutFundsWaitsForFunds() {
        Transfer big = accounts.transfer("client1", "client2", 150, 0);
        assertEquals(TransferStatus.PENDING, ledger.execute(big).get(0).status());
        assertEquals(100, ledger.balance("client1"));

        // A later transfer of the same account cannot overtake the blocked one
        Transfer small = accounts.transfer("client1", "client2", 1, 1);
        assertEquals(TransferStatus.PENDING, ledger.execute(small).get(0).status());

        // client3 sends funds to client1: both pending transfers are executed
        Transfer funds = accounts.transfer("client3", "client1", 60, 0);
        List<TransferOutcome> outcomes = ledger.execute(funds);
        assertEquals(3, outcomes.size());
        assertEquals(funds, outcomes.get(0).transfer());
        assertEquals(big, outcomes.get(1).transfer());
        assertEquals(small, outcomes.get(2).transfer());
        assertEquals(9, ledger.balance("client1"));
        assertEquals(251, ledger.balance("client2"));
        assertEquals(500, ledger.sumOfBalances());
    }

    @Test
    void cascadingExecutionAcrossAccounts() {
        // client2 and client3 are blocked waiting for funds
        ledger.execute(accounts.transfer("client2", "client3", 150, 0));
        ledger.execute(accounts.transfer("client3", "client4", 220, 0));
        assertEquals(2, ledger.pendingTransfers());

        // client1 -> client2 unblocks client2, which unblocks client3
        List<TransferOutcome> outcomes = ledger.execute(accounts.transfer("client1", "client2", 50, 0));
        assertEquals(3, outcomes.size());
        assertEquals(0, ledger.pendingTransfers());
        assertEquals(50, ledger.balance("client1"));
        assertEquals(0, ledger.balance("client2"));
        assertEquals(30, ledger.balance("client3"));
        assertEquals(320, ledger.balance("client4"));
    }

    @Test
    void duplicatesAreIgnored() {
        Transfer t = accounts.transfer("client1", "client2", 10, 0);
        ledger.execute(t);
        assertEquals(TransferStatus.EXECUTED, ledger.statusOf(t));
        List<TransferOutcome> again = ledger.execute(t);
        assertEquals(TransferStatus.DUPLICATE, again.get(0).status());
        assertEquals(90, ledger.balance("client1"));
        assertEquals(1, ledger.size());

        Transfer pending = accounts.transfer("client1", "client2", 10, 5);
        ledger.execute(pending);
        assertEquals(TransferStatus.DUPLICATE, ledger.statusOf(pending));
        assertEquals(TransferStatus.DUPLICATE, ledger.execute(pending).get(0).status());
        assertEquals(1, ledger.pendingTransfers());
    }

    @Test
    void conflictingTransfersOfSharedAccount() {
        // Both owners of the shared account issue a transfer with sequence number 0
        Transfer byClient1 = accounts.transfer("client1", "shared", "client3", 70, 0);
        Transfer byClient2 = accounts.transfer("client2", "shared", "client4", 70, 0);
        assertNotEquals(byClient1.id(), byClient2.id());

        assertEquals(TransferStatus.EXECUTED, ledger.execute(byClient1).get(0).status());
        assertEquals(TransferStatus.CONFLICT, ledger.statusOf(byClient2));
        assertEquals(TransferStatus.CONFLICT, ledger.execute(byClient2).get(0).status());
        assertEquals(30, ledger.balance("shared"));
        assertEquals(170, ledger.balance("client3"));
        assertEquals(100, ledger.balance("client4"));
    }

    @Test
    void conflictWithPendingTransfer() {
        Transfer first = accounts.transfer("client1", "client2", 1, 3);
        Transfer second = accounts.transfer("client1", "client3", 1, 3);
        assertEquals(TransferStatus.PENDING, ledger.execute(first).get(0).status());
        assertEquals(TransferStatus.CONFLICT, ledger.execute(second).get(0).status());
        assertEquals(1, ledger.pendingTransfers());
    }

    @Test
    void historyPagination() {
        for (int i = 0; i < 5; i++) {
            ledger.execute(accounts.transfer("client1", "client2", 1, i));
        }
        assertEquals(5, ledger.historySize("client1"));
        assertEquals(2, ledger.history("client1", 0, 2).size());
        assertEquals(1, ledger.history("client1", 4, 2).size());
        assertTrue(ledger.history("client1", 5, 2).isEmpty());
        assertTrue(ledger.history("client1", -1, 2).isEmpty());
        assertEquals(4, ledger.history("client1", 1, 100).get(3).seq());
    }

    @Test
    void stateDigestDependsOnlyOnBalancesAndSequenceNumbers() {
        Ledger other = accounts.newLedger(100);
        assertEquals(ledger.stateDigest(), other.stateDigest());

        Transfer a = accounts.transfer("client1", "client2", 5, 0);
        Transfer b = accounts.transfer("client3", "client4", 7, 0);
        ledger.execute(a);
        ledger.execute(b);
        assertNotEquals(ledger.stateDigest(), other.stateDigest());
        // Different order, same state
        other.execute(b);
        other.execute(a);
        assertEquals(ledger.stateDigest(), other.stateDigest());
    }

    @Test
    void hugeStringsDoNotBreakTheLedger() {
        String huge = "x".repeat(100_000);
        Transfer t = new Transfer(huge, "client2", 1, 0, huge, new byte[64]);
        assertEquals(64, t.id().length());
        assertInvalid(t);
    }

    @Test
    void digestDetectsDifferentConflictingTransfers() {
        // Same amount and destination, different signer: same balances, different transfers
        Ledger other = accounts.newLedger(100);
        ledger.execute(accounts.transfer("client1", "shared", "client3", 10, 0));
        other.execute(accounts.transfer("client2", "shared", "client3", 10, 0));
        assertEquals(ledger.balance("shared"), other.balance("shared"));
        assertNotEquals(ledger.stateDigest(), other.stateDigest());
    }

    private void assertInvalid(Transfer t) {
        assertFalse(ledger.isValid(t), t.toString());
        assertEquals(TransferStatus.INVALID, ledger.statusOf(t));
        assertEquals(TransferStatus.INVALID, ledger.execute(t).get(0).status());
    }
}
