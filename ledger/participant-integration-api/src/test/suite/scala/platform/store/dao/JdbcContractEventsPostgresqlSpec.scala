package com.daml.platform.store.dao

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

final class JdbcContractEventsPostgresqlSpec
    extends AsyncFlatSpec
    with Matchers
    with JdbcLedgerDaoSuite
    with JdbcLedgerDaoBackendPostgresql
    with JdbcLedgerDaoPackagesSpec
    with JdbcLedgerDaoContractEventsStreamSpec
    with JdbcPipelinedTransactionInsertion
