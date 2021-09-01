// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.Connection

import com.daml.lf.data.Ref
import com.daml.platform.server.api.ApiException
import com.daml.platform.store.appendonlydao.events.ContractId
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

private[backend] trait StorageBackendTestsMigrationPruning
    extends Matchers
    with StorageBackendSpec {
  this: AsyncFlatSpec =>

  import StorageBackendTestValues._

  it should "prune all divulgence events if pruning offset is after migration offset" in {
    val divulgee = Ref.Party.assertFromString("divulgee")
    val submitter = Ref.Party.assertFromString("submitter")

    val create = dtoCreate(offset(1), 1L, "#1", submitter)
    val divulgence = dtoDivulgence(None, 2L, "#1", submitter, divulgee)
    val archive = dtoExercise(offset(2), 3L, consuming = true, "#1", submitter)

    for {
      _ <- executeSql(backend.initializeParameters(someIdentityParams))
      _ <- executeSql(ingest(Vector(create, divulgence, archive), _))
      _ <- executeSql(backend.updateLedgerEnd(ParameterStorageBackend.LedgerEnd(offset(2), 3L)))
      // Simulate that the archive happened after the migration to append-only schema
      _ <- executeSql(updateMigrationHistoryTable(ledgerSequentialIdBefore = 2))
      beforePruning <- executeSql(
        backend.activeContractWithoutArgument(Set(divulgee), ContractId.assertFromString("#1"))
      )
      // Check that the divulgee can fetch the divulged event
      _ <- Future.successful(beforePruning should not be empty)
      // Trying to prune all divulged contracts before the migration should fail
      _ <-
        recoverToSucceededIf[ApiException](
          executeSql(
            backend.validatePruningOffsetAgainstMigration(
              offset(1),
              pruneAllDivulgedContracts = true,
              _,
            )
          )
        )
      // Validation passes the pruning offset for all divulged contracts is after the migration
      _ <- executeSql(
        backend.validatePruningOffsetAgainstMigration(
          offset(2),
          pruneAllDivulgedContracts = true,
          _,
        )
      )
      _ <- executeSql(
        backend.pruneEvents(offset(2), pruneAllDivulgedContracts = true)(_, loggingContext)
      )
      // Ensure the divulged contract is not visible anymore
      afterPruning <- executeSql(
        backend.activeContractWithoutArgument(Set(divulgee), ContractId.assertFromString("#1"))
      )
    } yield {
      // Pruning succeeded
      afterPruning shouldBe empty
    }
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
