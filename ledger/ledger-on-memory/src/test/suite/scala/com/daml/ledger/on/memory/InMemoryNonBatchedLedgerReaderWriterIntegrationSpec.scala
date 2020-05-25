package com.daml.ledger.on.memory

class InMemoryNonBatchedLedgerReaderWriterIntegrationSpec
    extends InMemoryBatchedLedgerReaderWriterIntegrationSpec(enableBatching = false) {}
