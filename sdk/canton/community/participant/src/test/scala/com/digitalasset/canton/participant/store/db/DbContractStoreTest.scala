// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
      pByIid <- store.lookupBatchedNonReadThrough(Seq(p.value.internalContractId)).failOnShutdown
      c <- store.lookupE(contractId)
    } yield {
      p0 shouldBe None
      c shouldEqual contract
      p.value.asContractInstance shouldEqual contract
      pByIid.get(p.value.internalContractId) shouldEqual p
      store.lookupPersistedIfCached(contractId).value.value.asContractInstance shouldEqual contract
      store.lookupPersistedIfCached(p.value.internalContractId) shouldEqual store
        .lookupPersistedIfCached(contractId)
    }
  }

  "store and retrieve created contracts by their internal contract ids" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    store.lookupPersistedIfCached(contractId) shouldBe None

    val contracts = Seq(contract, contract2, contract3, contract4)

    for {
      internalIdsFromStore <- store
        .storeContracts(contracts)
        .failOnShutdown
      internalContractIds = internalIdsFromStore.values
      contractIdsFromStore = internalIdsFromStore.map(_.swap)
      contractIdsCached <- store
        .lookupBatchedContractIdsNonReadThrough(internalContractIds)
        .failOnShutdown
      contractIdsNonCached <- store
        .lookupBatchedContractIdsFromPersistence(internalContractIds)
        .failOnShutdown
    } yield {
      contractIdsFromStore.values should contain theSameElementsAs contracts.map(_.contractId)
      internalContractIds.toSet.size shouldBe internalContractIds.size
      contractIdsCached should contain theSameElementsAs contractIdsFromStore
      contractIdsNonCached should contain theSameElementsAs contractIdsFromStore
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
      p = store.lookupPersistedIfCached(contractId)
      internalContractId = p.value.value.internalContractId
      internalContractId5 = store
        .lookupPersistedIfCached(contractId5)
        .value
        .value
        .internalContractId
      _ = p.value.nonEmpty shouldBe true
      _ = store.lookupPersistedIfCached(internalContractId) shouldEqual p
      _ <- store
        .deleteIgnoringUnknown(Seq(contractId, contractId2, contractId3, contractId4))
        .failOnShutdown
      _ = store.lookupPersistedIfCached(contractId) shouldBe None
      _ = store.lookupPersistedIfCached(internalContractId) shouldBe None
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
      // the entry from the internal id cache is removed when the contract is deleted
      store.lookupPersistedIfCached(internalContractId) shouldBe None
      store.lookupPersistedIfCached(contractId5).value.nonEmpty shouldBe true
      store.lookupPersistedIfCached(internalContractId5) shouldEqual store.lookupPersistedIfCached(
        contractId5
      )
    }
  }

  "storeContracts handles empty list" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )
    for {
      result <- store.storeContracts(Seq.empty).failOnShutdown
    } yield {
      result shouldBe Map.empty
    }
  }

  "storeContracts preserves contract-to-result order mapping" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )
    val contracts = Seq(contract, contract2, contract3, contract4, contract5)

    for {
      internalIds <- store.storeContracts(contracts).failOnShutdown
      // Verify all contracts got internal IDs
      _ = contracts.foreach { c =>
        internalIds.get(c.contractId) shouldBe defined
      }
      // Verify we can retrieve all contracts by their internal IDs
      retrievedContracts <- store.lookupBatchedFromPersistence(internalIds.values).failOnShutdown
    } yield {
      retrievedContracts.size shouldBe contracts.size
      // Each contract should map to its correct internal ID
      contracts.foreach { c =>
        val internalId = internalIds(c.contractId)
        val retrieved = retrievedContracts(internalId)
        retrieved.inst.contractId shouldBe c.contractId
      }
      succeed
    }
  }

  "storeContracts handles duplicate contracts in the same batch" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )
    // Test with duplicates in the same batch: contract appears twice, contract2 appears three times
    val duplicateContracts = Seq(contract, contract2, contract, contract3, contract2, contract2)

    for {
      internalIds <- store.storeContracts(duplicateContracts).failOnShutdown
    } yield {
      // Should return internal IDs for all unique contracts only
      internalIds should have size 3
      internalIds.keys should contain theSameElementsAs Set(
        contractId,
        contractId2,
        contractId3,
      )

      // Verify each contract has a valid and distinct internal ID
      internalIds.values.foreach { internalId =>
        internalId should be > 0L
      }
      internalIds.values.toSeq.distinct should have size 3
      succeed
    }
  }

  "storeContracts handles mix of new and existing contracts with duplicates" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    for {
      // First, store some contracts
      firstBatch <- store.storeContracts(Seq(contract, contract2)).failOnShutdown
      // Then store a batch with duplicates including already-stored contracts
      secondBatch <- store
        .storeContracts(Seq(contract, contract2, contract3, contract, contract3))
        .failOnShutdown
    } yield {
      // First batch should have 2 contracts
      firstBatch should have size 2
      // Second batch should have all 3 unique contracts
      secondBatch should have size 3
      secondBatch.keys should contain theSameElementsAs Set(
        contractId,
        contractId2,
        contractId3,
      )
      // Internal IDs for already-existing contracts should match
      secondBatch(contractId) shouldBe firstBatch(contractId)
      secondBatch(contractId2) shouldBe firstBatch(contractId2)
    }
  }

  "storeContracts populates cache for all inserted contracts" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    // Verify contracts are not cached initially
    store.lookupPersistedIfCached(contractId) shouldBe None
    store.lookupPersistedIfCached(contractId2) shouldBe None

    for {
      // Store contracts
      _ <- store.storeContracts(Seq(contract, contract2, contract3)).failOnShutdown
    } yield {
      // Verify they're now cached
      val cached1 = store.lookupPersistedIfCached(contractId)
      val cached2 = store.lookupPersistedIfCached(contractId2)
      val cached3 = store.lookupPersistedIfCached(contractId3)

      cached1 shouldBe defined
      cached2 shouldBe defined
      cached3 shouldBe defined
      cached1.value.value.asContractInstance shouldBe contract
      cached2.value.value.asContractInstance shouldBe contract2
      cached3.value.value.asContractInstance shouldBe contract3
    }
  }

  "storeContracts utilizes cache on lookups" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    for {
      // Store contracts (populates cache)
      _ <- store.storeContracts(Seq(contract, contract2)).failOnShutdown
      // Verify contracts are cached
      _ = store.lookupPersistedIfCached(contractId).value shouldBe defined
      _ = store.lookupPersistedIfCached(contractId2).value shouldBe defined
      // Lookup should use cache (not hit database again)
      looked1 <- store.lookupPersisted(contractId).failOnShutdown
      looked2 <- store.lookupPersisted(contractId2).failOnShutdown
      // lookupMetadata should also benefit from cache
      metadata <- store.lookupMetadata(Set(contractId, contractId2)).value.failOnShutdown
    } yield {
      looked1 shouldBe defined
      looked2 shouldBe defined
      looked1.value.asContractInstance shouldBe contract
      looked2.value.asContractInstance shouldBe contract2

      val metadataMap = metadata.value
      metadataMap(contractId) shouldBe contract.metadata
      metadataMap(contractId2) shouldBe contract2.metadata

      // Cache should still contain the contracts
      store.lookupPersistedIfCached(contractId).value shouldBe defined
      store.lookupPersistedIfCached(contractId2).value shouldBe defined
    }
  }

  "lookupMetadata uses cache instead of database when available" in {
    import storage.api.*
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    for {
      // Store contracts and populate cache
      _ <- store.storeContracts(Seq(contract, contract2)).failOnShutdown
      // Verify cached
      _ = store.lookupPersistedIfCached(contractId) shouldBe defined
      _ = store.lookupPersistedIfCached(contractId2) shouldBe defined
      // Delete contracts from database (but keep in cache!)
      _ <- storage.update_(sqlu"delete from par_contracts", "test-delete").failOnShutdown
      // Verify database is empty
      count <- storage
        .query(sql"select count(*) from par_contracts".as[Int].head, "test-count")
        .failOnShutdown
      _ = count shouldBe 0
      // lookupMetadata should still work using cache
      metadata <- store.lookupMetadata(Set(contractId, contractId2)).value.failOnShutdown
    } yield {
      // If cache wasn't used, this would fail (database is empty)
      val metadataMap = metadata.value
      metadataMap should have size 2
      metadataMap(contractId) shouldBe contract.metadata
      metadataMap(contractId2) shouldBe contract2.metadata
    }
  }

  "lookupMetadata returns correct metadata for multiple contracts" in {
    val store = createDbContractStoreForTesting(
      storage,
      loggerFactory,
    )

    for {
      // Store multiple contracts
      _ <- store
        .storeContracts(Seq(contract, contract2, contract3, contract4, contract5))
        .failOnShutdown
      // Lookup metadata for all
      metadata <- store
        .lookupMetadata(Set(contractId, contractId2, contractId3, contractId4, contractId5))
        .value
        .failOnShutdown
    } yield {
      val metadataMap = metadata.value
      // Verify all metadata is correct (order is preserved in batch processing)
      metadataMap should have size 5
      metadataMap(contractId) shouldBe contract.metadata
      metadataMap(contractId2) shouldBe contract2.metadata
      metadataMap(contractId3) shouldBe contract3.metadata
      metadataMap(contractId4) shouldBe contract4.metadata
      metadataMap(contractId5) shouldBe contract5.metadata
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
