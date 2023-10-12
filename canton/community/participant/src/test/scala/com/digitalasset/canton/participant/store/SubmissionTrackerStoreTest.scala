// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.{BaseTest, TestMetrics}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait SubmissionTrackerStoreTest
    extends AsyncWordSpec
    with BaseTest
    with TestMetrics
    with PrunableByTimeTest {
  private def mkHash(data: String): Hash = Hash
    .build(TestHash.testHashPurpose, HashAlgorithm.Sha256)
    .addWithoutLengthPrefix(data)
    .finish()

  lazy val rootHashes =
    Seq("falafel", "hummus", "tahini", "fattoush", "tabbouleh").map(seed => RootHash(mkHash(seed)))
  lazy val requestIds =
    (1 to rootHashes.size).map(i => RequestId(CantonTimestamp.Epoch.plusSeconds(i.toLong)))
  lazy val maxSeqTimes =
    (1 to rootHashes.size).map(i => CantonTimestamp.Epoch.plusSeconds(i.toLong + 10))

  def submissionTrackerStore(mkStore: () => SubmissionTrackerStore): Unit = {
    behave like prunableByTime(_ => mkStore())

    "when registering" should {
      "confirm a first registration is fresh" in {
        val store = mkStore()

        for {
          result <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
        } yield {
          result shouldBe Outcome(true)
        }
      }

      "be idempotent" in {
        val store = mkStore()

        for {
          _ <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
          result <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
        } yield {
          result shouldBe Outcome(true)
        }
      }

      "signal a replay when registering a different request for an existing root hash" in {
        val store = mkStore()

        for {
          first <- store.registerFreshRequest(rootHashes(0), requestIds(0), maxSeqTimes(0)).unwrap
          second <- store.registerFreshRequest(rootHashes(0), requestIds(1), maxSeqTimes(1)).unwrap
        } yield {
          first shouldBe Outcome(true)
          second shouldBe Outcome(false)
        }
      }
    }

    "delete entries when pruning" in {
      val store = mkStore()

      val pruningTs = maxSeqTimes(1)
      val expectedCountAfterPrune = maxSeqTimes.count(_ > pruningTs)

      for {
        initialCount <- store.size.unwrap

        _ <- Future.sequence(rootHashes.indices.map { i =>
          store.registerFreshRequest(rootHashes(i), requestIds(i), maxSeqTimes(i)).unwrap
        })
        finalCount <- store.size.unwrap

        _ <- store.prune(pruningTs)
        countAfterPrune <- store.size.unwrap
      } yield {
        initialCount shouldBe Outcome(0)
        finalCount shouldBe Outcome(rootHashes.size)
        countAfterPrune shouldBe Outcome(expectedCountAfterPrune)
      }
    }

    "delete entries when cleaning up" in {
      val store = mkStore()

      val cleanupTs = requestIds(3).unwrap
      val expectedCountAfterDelete = requestIds.count(_.unwrap < cleanupTs)

      for {
        initialCount <- store.size.unwrap

        _ <- Future.sequence(rootHashes.indices.map { i =>
          store.registerFreshRequest(rootHashes(i), requestIds(i), maxSeqTimes(i)).unwrap
        })
        finalCount <- store.size.unwrap

        _ <- store.deleteSince(cleanupTs)
        countAfterDelete <- store.size.unwrap
      } yield {
        initialCount shouldBe Outcome(0)
        finalCount shouldBe Outcome(rootHashes.size)
        countAfterDelete shouldBe Outcome(expectedCountAfterDelete)
      }
    }
  }

}
