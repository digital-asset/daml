// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.BatchingConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.store.ActiveContractStoreTest
import com.digitalasset.canton.participant.store.db.DbActiveContractStoreTest.maxSynchronizerIndex
import com.digitalasset.canton.participant.store.db.DbContractStoreTest.createDbContractStoreForTesting
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.store.{
  IndexedStringType,
  IndexedSynchronizer,
  PrunableByTimeParameters,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, ReassignmentCounter, RepairCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

trait DbActiveContractStoreTest extends AsyncWordSpec with BaseTest with ActiveContractStoreTest {
  this: DbTest =>

  protected val synchronizerIndex = 1

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_active_contracts",
        sqlu"truncate table par_active_contract_pruning",
        sqlu"delete from par_contracts",
      ),
      functionFullName,
    )
  }

  "DbActiveContractStore" should {
    behave like activeContractStore(
      ec => {
        val indexStore =
          new InMemoryIndexedStringStore(minIndex = 1, maxIndex = maxSynchronizerIndex)

        val synchronizerId = IndexedSynchronizer.tryCreate(
          acsSynchronizerId,
          indexStore.getOrCreateIndexForTesting(
            IndexedStringType.synchronizerId,
            acsSynchronizerStr,
          ),
        )
        // Check we end up with the expected synchronizer index. If we don't, then test isolation may get broken.
        assert(synchronizerId.index == synchronizerIndex)
        new DbActiveContractStore(
          storage,
          synchronizerId,
          enableAdditionalConsistencyChecks = Some(1000),
          PrunableByTimeParameters.testingParams,
          BatchingConfig(),
          indexStore,
          timeouts,
          loggerFactory,
        )(ec)
      },
      ec =>
        createDbContractStoreForTesting(
          storage,
          loggerFactory,
        )(ec),
    )
  }
}

private[db] object DbActiveContractStoreTest {

  /** Limit the range of synchronizer indices that the ActiveContractStoreTest can use, to
    * future-proof against interference with the ContractStoreTest. Currently, the
    * ActiveContractStoreTest only needs to reserve the first index 1.
    */
  val maxSynchronizerIndex: Int = 100
}

class ActiveContractStoreTestH2 extends DbActiveContractStoreTest with H2Test

class ActiveContractStoreTestPostgres extends DbActiveContractStoreTest with PostgresTest {
  // this test doesn't work with H2, because it doesn't support arrays with more than 2^16 elements
  "be able to handle many changes within a time period" inUS {

    val indexStore =
      new InMemoryIndexedStringStore(minIndex = 1, maxIndex = maxSynchronizerIndex)

    val synchronizerId = IndexedSynchronizer.tryCreate(
      acsSynchronizerId,
      indexStore.getOrCreateIndexForTesting(
        IndexedStringType.synchronizerId,
        acsSynchronizerStr,
      ),
    )
    // Check we end up with the expected synchronizer index. If we don't, then test isolation may get broken.
    assert(synchronizerId.index == synchronizerIndex)
    val acs = new DbActiveContractStore(
      storage,
      synchronizerId,
      enableAdditionalConsistencyChecks = None,
      PrunableByTimeParameters.testingParams,
      BatchingConfig(),
      indexStore,
      timeouts,
      loggerFactory,
    )(executionContext)

    val rc = None: Option[RepairCounter]
    val rc1 = Some(RepairCounter.One): Option[RepairCounter]

    val ts = CantonTimestamp.assertFromInstant(Instant.parse("2019-04-04T10:00:00.00Z"))
    val ts1 = ts.addMicros(1)

    val toc1 = TimeOfChange(ts, rc)
    val toc2 = TimeOfChange(ts1, rc1)

    val many =
      (1 to 100_000).toList.map(n => ExampleTransactionFactory.suffixedId(n, n))
    for {

      _ <- valueOrFail(
        MonadUtil.batchedSequentialTraverse_(PositiveInt.one, PositiveInt.tryCreate(5000))(
          many.map((_, ReassignmentCounter(0)))
        )(acs.markContractsCreated(_, toc1))
      )("create many in chunks")

      _ <- valueOrFail(
        MonadUtil.batchedSequentialTraverse_(PositiveInt.one, PositiveInt.tryCreate(5000))(
          many
        )(acs.archiveContracts(_, toc2))
      )("archive many in chunks")

      manyChanges <- acs.changesBetween(toc1, toc2)
    } yield {
      manyChanges.flatMap { case (_, changes) =>
        changes.deactivations.keys
      }.size shouldBe many.size.toLong
    }
  }

}
