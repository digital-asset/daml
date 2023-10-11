// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Connection

private[backend] trait StorageBackendTestsMigrationPruning
    extends Matchers
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  private val cid = hashCid("#1")

  it should "prune all divulgence events if pruning offset is after migration offset" in {
    val divulgee = Ref.Party.assertFromString("divulgee")
    val submitter = Ref.Party.assertFromString("submitter")

    val create = dtoCreate(offset(1), 1L, cid, submitter)
    val divulgence = dtoDivulgence(None, 2L, cid, submitter, divulgee)
    val archive = dtoExercise(offset(2), 3L, consuming = true, cid, submitter)

    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(Vector(create, divulgence, archive), _))
    executeSql(updateLedgerEnd(offset(2), 3L))

    // Simulate that the archive happened after the migration to append-only schema
    executeSql(updateMigrationHistoryTable(ledgerSequentialIdBefore = 2))
    val beforePruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        cid,
      )
    )

    // Check that the divulgee can fetch the divulged event
    beforePruning should not be empty

    // Trying to prune all divulged contracts before the migration should fail
    executeSql(
      backend.event.isPruningOffsetValidAgainstMigration(
        offset(1),
        pruneAllDivulgedContracts = true,
        _,
      )
    ) shouldBe false

    // Validation passes the pruning offset for all divulged contracts is after the migration
    executeSql(
      backend.event.isPruningOffsetValidAgainstMigration(
        offset(2),
        pruneAllDivulgedContracts = true,
        _,
      )
    ) shouldBe true

    executeSql(
      backend.event.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(
        _,
        traceContext,
      )
    )

    // Ensure the divulged contract is not visible anymore
    val afterPruning = executeSql(
      backend.contract.activeContractWithoutArgument(
        Set(divulgee),
        cid,
      )
    )

    // Pruning succeeded
    afterPruning shouldBe empty
  }

  private def updateMigrationHistoryTable(ledgerSequentialIdBefore: Long) = { conn: Connection =>
    val statement = conn.prepareStatement(
      "UPDATE participant_migration_history_v100 SET ledger_end_sequential_id_before = ?"
    )
    statement.setLong(1, ledgerSequentialIdBefore)
    statement.executeUpdate()
    statement.close()
  }
}
