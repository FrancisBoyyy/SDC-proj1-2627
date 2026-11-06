package app.ledger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import app.messages.Transfer;

class TransferOutcomeJournalTest {

    private TestAccounts accounts;
    private TransferOutcomeJournal journal;

    @BeforeEach
    void setUp() throws Exception {
        accounts = new TestAccounts(4);
        journal = new TransferOutcomeJournal();
    }

    @Test
    void recordsTransferObservationsInOrder() {
        Transfer valid = accounts.transfer("client1", "client2", 10, 0);
        Transfer invalidSignature = new Transfer(
                valid.from(),
                valid.to(),
                valid.amount(),
                valid.seq(),
                valid.signer(),
                new byte[] { 1, 2, 3 }
        );

        // The transfer ID intentionally matches: it excludes signature bytes.
        assertEquals(valid.id(), invalidSignature.id());

        journal.recordClientRequest(invalidSignature);
        journal.recordRequestStatus(
                invalidSignature, TransferStatus.INVALID, -1);

        journal.recordClientRequest(valid);
        journal.recordReplicationSubmission(valid);
        journal.recordLedgerOutcome(
                new TransferOutcome(valid, TransferStatus.EXECUTED, 0));

        var events = journal.snapshot();

        assertEquals(5, events.size());

        assertEquals(0, events.get(0).ordinal());
        assertEquals(
                TransferAuditEvent.Stage.CLIENT_REQUEST,
                events.get(0).stage());

        assertEquals(
                TransferAuditEvent.Stage.REQUEST_STATUS,
                events.get(1).stage());
        assertEquals(
                Optional.of(TransferStatus.INVALID),
                events.get(1).status());

        assertEquals(
                TransferAuditEvent.Stage.REPLICATION_SUBMISSION,
                events.get(3).stage());

        assertEquals(
                TransferAuditEvent.Stage.LEDGER_OUTCOME,
                events.get(4).stage());
        assertEquals(
                Optional.of(TransferStatus.EXECUTED),
                events.get(4).status());
        assertEquals(0, events.get(4).ledgerIndex());

        // Same operation ID, different signature bytes: the audit distinguishes them.
        assertNotEquals(
                events.get(0).signatureDigest(),
                events.get(2).signatureDigest());
    }

    @Test
    void journalSnapshotIsImmutable() {
        Transfer transfer = accounts.transfer("client1", "client2", 10, 0);
        journal.recordClientRequest(transfer);

        var events = journal.snapshot();

        assertThrows(
                UnsupportedOperationException.class,
                () -> events.add(events.get(0)));
    }
}