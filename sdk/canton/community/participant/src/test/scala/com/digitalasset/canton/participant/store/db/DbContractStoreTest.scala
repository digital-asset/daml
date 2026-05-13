// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.syntax.parallel.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  CachingConfigs,
  DefaultProcessingTimeouts,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.store.db.DbContractStoreTest.createDbContractStoreForTesting
import com.digitalasset.canton.participant.store.{ContractStoreTest, UnknownContract}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.{DbStorageIdempotency, DbTest, H2Test, PostgresTest}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.ExecutionContext

trait DbContractStoreTest extends AsyncWordSpec with BaseTest with ContractStoreTest {
  this: DbTest =>

  // Ensure this test can't interfere with the ActiveContractStoreTest
  protected lazy val synchronizerIndex: Int = DbActiveContractStoreTest.maxSynchronizerIndex + 1

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Int] = {
    import storage.api.*
    storage.update(
      sqlu"delete from par_contracts",
      functionFullName,
    )
  }

  "DbContractStore" should {
    behave like contractStore(() =>
      createDbContractStoreForTesting(
        storage,
        loggerFactory,
      )
    )
  }

  "store and retrieve a created contract with correct cache behavior" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    store.lookupPersistedIfCached(contractId) shouldBe None
    store.lookupPersistedIfCached(
      contractId
    ) shouldBe None // should not cache IfCached lookup result

    for {
      p0 <- store.lookupPersisted(contractId).failOnShutdown
      _ <- store.lookupPersistedIfCached(contractId) shouldBe Some(None)
      _ <- store.storeContract(contract).failOnShutdown
      p <- store.lookupPersisted(contractId).failOnShutdown
      c <- store.lookupE(contractId)
    } yield {
      p0 shouldBe None
      c shouldEqual contract
      p.value.asContractInstance shouldEqual contract
      store.lookupPersistedIfCached(contractId).value.value.asContractInstance shouldEqual contract
    }
  }

  "delete a set of contracts as done by pruning with correct cache behavior" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )
    store.lookupPersistedIfCached(contractId) shouldBe None
    for {
      _ <- List(contract, contract2, contract4, contract5)
        .parTraverse(store.storeContract)
        .failOnShutdown
      _ = store.lookupPersistedIfCached(contractId).value.nonEmpty shouldBe true
      _ <- store
        .deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
        .failOnShutdown
      _ = store.lookupPersistedIfCached(contractId) shouldBe None
      notFounds <- List(contractId, contractId2, contractId3, contractId4).parTraverse(
        store.lookupE(_).value
      )
      notDeleted <- store.lookupE(contractId5).value
    } yield {
      notFounds shouldEqual List(
        Left(UnknownContract(contractId)),
        Left(UnknownContract(contractId2)),
        Left(UnknownContract(contractId3)),
        Left(UnknownContract(contractId4)),
      )
      notDeleted shouldEqual Right(contract5)
      store.lookupPersistedIfCached(contractId) shouldBe Some(None) // already tried to be looked up
      store.lookupPersistedIfCached(contractId5).value.nonEmpty shouldBe true
    }
  }
}
object DbContractStoreTest {

  def createDbContractStoreForTesting(
      storage: DbStorageIdempotency,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DbContractStore =
    new DbContractStore(
      storage = storage,
      cacheConfig = CachingConfigs.testing.contractStore,
      dbQueryBatcherConfig = BatchAggregatorConfig.defaultsForTesting,
      insertBatchAggregatorConfig = BatchAggregatorConfig.defaultsForTesting,
      timeouts = DefaultProcessingTimeouts.testing,
      loggerFactory = loggerFactory,
    )
}

class ContractStoreTestH2 extends DbContractStoreTest with H2Test

class ContractStoreTestPostgres extends DbContractStoreTest with PostgresTest
