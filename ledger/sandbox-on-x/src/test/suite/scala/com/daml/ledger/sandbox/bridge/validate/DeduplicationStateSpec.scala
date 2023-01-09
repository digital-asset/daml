// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import java.time.Duration

import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.bridge.validate.DeduplicationState.DeduplicationStateQueueMap
import com.daml.lf.data.{Ref, Time}
import com.daml.metrics.Metrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.VectorMap
import scala.util.chaining._
import scala.util.{Failure, Success, Try}

class DeduplicationStateSpec extends AnyFlatSpec with Matchers {
  behavior of classOf[DeduplicationState].getSimpleName

  private val t0 = Time.Timestamp.now()
  private val t1 = t0.add(Duration.ofMinutes(1L))
  private val t2 = t0.add(Duration.ofMinutes(2L))
  private val t3 = t0.add(Duration.ofMinutes(3L))

  private val bridgeMetrics = new BridgeMetrics(Metrics.ForTesting)

  it should "deduplicate commands within the requested deduplication window" in {
    val deduplicationState = DeduplicationState.empty(
      deduplicationDuration = Duration.ofMinutes(3L),
      bridgeMetrics = bridgeMetrics,
    )

    deduplicationState
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(2L),
        recordTime = t0,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(1) -> t0
        )
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(2L),
        recordTime = t1,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(1) -> t0
        )
        isDuplicate shouldBe true
      }
      ._1
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(2L),
        recordTime = t3,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(1) -> t3
        )
        isDuplicate shouldBe false
      }
  }

  it should "evict old entries (older than max deduplication duration)" in {
    val deduplicationState = DeduplicationState.empty(
      deduplicationDuration = Duration.ofMinutes(1L),
      bridgeMetrics = bridgeMetrics,
    )

    deduplicationState
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(1L),
        recordTime = t0,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(1) -> t0
        )
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(
        changeId = changeId(2),
        commandDeduplicationDuration = Duration.ofMinutes(1L),
        recordTime = t1,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(1) -> t0,
          changeId(2) -> t1,
        )
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(
        changeId = changeId(3),
        commandDeduplicationDuration = Duration.ofMinutes(1L),
        recordTime = t2,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue.assertToVectorMap shouldBe VectorMap(
          changeId(2) -> t1,
          changeId(3) -> t2,
        )
        isDuplicate shouldBe false
      }
  }

  it should "throw an exception on too big requested deduplication duration" in {
    val maxDeduplicationDuration = Duration.ofMinutes(2L)
    val commandDeduplicationDuration = maxDeduplicationDuration.plus(Duration.ofSeconds(1L))
    Try(
      DeduplicationState
        .empty(
          deduplicationDuration = maxDeduplicationDuration,
          bridgeMetrics = bridgeMetrics,
        )
        .deduplicate(changeId(1337), commandDeduplicationDuration, t0)
    ) match {
      case Failure(ex) =>
        ex.getMessage shouldBe s"assertion failed: Cannot deduplicate for a period ($commandDeduplicationDuration) longer than the max deduplication duration ($maxDeduplicationDuration)."
      case Success(_) => fail("It should throw an exception on invalid deduplication durations")
    }
  }

  it should "throw an exception on a record time before the last ingested record time" in {
    val maxDeduplicationDuration = Duration.ofMinutes(2L)
    Try(
      DeduplicationState
        .empty(
          deduplicationDuration = maxDeduplicationDuration,
          bridgeMetrics = bridgeMetrics,
        )
        .deduplicate(changeId(1337), maxDeduplicationDuration, t1)
        ._1
        .deduplicate(changeId(1337), maxDeduplicationDuration, t0)
    ) match {
      case Failure(ex) =>
        ex.getMessage shouldBe s"assertion failed: Inserted record time ($t0) for changeId (${changeId(1337)}) cannot be before the last inserted record time (${Some(t1)})."
      case Success(_) => fail("It should throw an exception on invalid deduplication durations")
    }
  }

  behavior of classOf[DeduplicationStateQueueMap].getSimpleName

  it should "update and expire entries properly" in {
    DeduplicationStateQueueMap.empty
      .updated(changeId(1), t0)
      .updated(changeId(2), t1)
      // Insert duplicate changeId at t2
      .updated(changeId(1), t2)
      // Assert deduplication entries for both data structures
      .tap { queueMap =>
        queueMap.mappings should contain theSameElementsAs Map(
          changeId(2) -> t1,
          changeId(1) -> t2,
        )
        queueMap.vector should contain theSameElementsAs Vector(
          changeId(1) -> t0,
          changeId(2) -> t1,
          changeId(1) -> t2,
        )
      }
      .withoutOlderThan(t1)
      // The first deduplication entry is removed from the vector
      // but its change-id is not removed from the mapping since it was updated more recently
      .tap { queueMap =>
        val expectedElements = Map(
          changeId(2) -> t1,
          changeId(1) -> t2,
        )
        queueMap.mappings should contain theSameElementsAs expectedElements
        queueMap.vector should contain theSameElementsAs expectedElements
      }
      .withoutOlderThan(t2)
      // The change-id at t1 is evicted from both data structures
      .tap { queueMap =>
        queueMap.mappings should contain theSameElementsAs Map(changeId(1) -> t2)
        queueMap.vector should contain theSameElementsAs Map(changeId(1) -> t2)
      }
      .withoutOlderThan(t3)
      // All entries evicted
      .tap { queueMap =>
        queueMap.mappings shouldBe empty
        queueMap.vector shouldBe empty
      }
  }

  private def changeId(idx: Int): ChangeId = ChangeId(
    applicationId = Ref.ApplicationId.assertFromString("some-app"),
    commandId = Ref.CommandId.assertFromString(s"some-command-$idx"),
    actAs = Set.empty,
  )

  private implicit class DeduplicationStateOps(val underlying: DeduplicationStateQueueMap) {
    // Assert that the two internal data structures contain the same mappings
    def assertToVectorMap: VectorMap[ChangeId, Time.Timestamp] =
      underlying.vector
        .foldLeft(VectorMap.empty[ChangeId, Time.Timestamp]) { case (res, (chgId, recordTime)) =>
          res.updated(chgId, recordTime)
        }
        .tap { mapFromVector =>
          if (mapFromVector != underlying.mappings)
            throw new RuntimeException(
              s"Mappings ${underlying.mappings} != vector entries: ${underlying.vector}"
            )
        }
        .map { case (chgId, recordTime) => chgId -> recordTime }
  }
}
