// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data

import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.EpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.digitalasset.canton.tracing.Traced
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait AvailabilityStoreTest extends AsyncWordSpec with BftSequencerBaseTest {
  def createStore(): AvailabilityStore[PekkoEnv]

  private val request1 = Traced(
    OrderingRequest("tag", ByteString.copyFromUtf8("payload1"))
  )
  private val request2 = Traced(
    OrderingRequest("tag", ByteString.copyFromUtf8("payload2"))
  )
  private val request3 = Traced(
    OrderingRequest("tag", ByteString.copyFromUtf8("payload3"))
  )

  private val batch1 = OrderingRequestBatch.create(Seq(request1, request3), EpochNumber.First)
  private val batch1Id = BatchId.from(batch1)
  private val batch2 = OrderingRequestBatch.create(Seq(request2, request3), EpochNumber(1L))
  private val batch2Id = BatchId.from(batch2)
  private val batchEmpty = OrderingRequestBatch.create(Seq(), EpochNumber(2L))
  private val batchEmptyId = BatchId.from(batchEmpty)

  private val missingBatchId1 = BatchId.createForTesting("A missing batchId")

  "AvailbilityStore" should {
    "fail retrieve non-inserted batchId" in {
      val store = createStore()

      for {
        fetchedBatch <- store.fetchBatches(Seq(missingBatchId1))
      } yield {
        fetchedBatch shouldBe AvailabilityStore.MissingBatches(Set(missingBatchId1))
      }
    }

    "create and retrieve batches" in {
      val store = createStore()

      for {
        _ <- store.addBatch(batch1Id, batch1)
        fetchedBatch <- store.fetchBatches(Seq(batch1Id))
      } yield {
        fetchedBatch shouldBe AvailabilityStore.AllBatches(Seq(batch1Id -> batch1))
      }
    }

    "create and retrieve empty batches" in {
      val store = createStore()

      for {
        _ <- store.addBatch(batchEmptyId, batchEmpty)
        fetchedBatch <- store.fetchBatches(Seq(batchEmptyId))
      } yield {
        fetchedBatch shouldBe AvailabilityStore.AllBatches(Seq(batchEmptyId -> batchEmpty))
      }
    }

    "fetch empty set of batches" in {
      val store = createStore()

      for {
        fetchedBatches <- store.fetchBatches(Seq.empty)
      } yield fetchedBatches shouldBe AvailabilityStore.AllBatches(Seq.empty)
    }

    "fetch several batches" should {
      "return AllBatches if all are in store" in {
        val store = createStore()

        for {
          _ <- store.addBatch(batch1Id, batch1)
          _ <- store.addBatch(batch2Id, batch2)
          _ <- store.addBatch(batchEmptyId, batchEmpty)
          batches <- store.fetchBatches(Seq(batch1Id, batchEmptyId, batch2Id))
        } yield {
          batches shouldBe AvailabilityStore.AllBatches(
            Seq(batch1Id -> batch1, batchEmptyId -> batchEmpty, batch2Id -> batch2)
          )
        }
      }

      "return MissingBatches if some batches are missing" in {
        val store = createStore()

        for {
          _ <- store.addBatch(batch1Id, batch1)
          _ <- store.addBatch(batchEmptyId, batchEmpty)
          batches <- store.fetchBatches(Seq(batch1Id, batchEmptyId, missingBatchId1))
        } yield {
          batches shouldBe AvailabilityStore.MissingBatches(Set(missingBatchId1))
        }
      }

      "can handle duplicate batch IDs" in {
        val store = createStore()

        for {
          _ <- store.addBatch(batch1Id, batch1)
          batches <- store.fetchBatches(Seq(missingBatchId1, batch1Id, batch1Id))
        } yield {
          batches shouldBe AvailabilityStore.MissingBatches(Set(missingBatchId1))
        }
      }

      "addBatch twice keeps first one" in {
        val store = createStore()

        for {
          _ <- store.addBatch(batch1Id, batch1)
          _ <- store.addBatch(batch1Id, batch2)
          batches <- store.fetchBatches(Seq(batch1Id))
        } yield {
          batches shouldBe AvailabilityStore.AllBatches(Seq(batch1Id -> batch1))
        }
      }

      "prune batches" in {
        val store = createStore()

        val batch1 = OrderingRequestBatch.create(Seq(request1), EpochNumber.First)
        val batch1Id = BatchId.from(batch1)

        val batch2 = OrderingRequestBatch.create(Seq(request1), EpochNumber(1L))
        val batch2Id = BatchId.from(batch2)

        val batch3 = OrderingRequestBatch.create(Seq(request1), EpochNumber(2L))
        val batch3Id = BatchId.from(batch3)

        for {
          _ <- store.addBatch(batch1Id, batch1)
          _ <- store.addBatch(batch2Id, batch2)
          _ <- store.addBatch(batch3Id, batch3)

          numberOfRecords <- store.loadNumberOfRecords

          _ = numberOfRecords.batches shouldBe (3)

          _ <- store.prune(EpochNumber(2L))

          numberOfRecordsAfterPruning <- store.loadNumberOfRecords

        } yield numberOfRecordsAfterPruning.batches shouldBe (1)
      }
    }
  }
}
