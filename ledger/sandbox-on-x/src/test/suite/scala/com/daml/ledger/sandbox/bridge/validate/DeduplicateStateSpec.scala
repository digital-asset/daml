// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.lf.data.{Ref, Time}
import com.daml.metrics.Metrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.collection.immutable.VectorMap
import scala.util.{Failure, Success, Try}
import scala.util.chaining._

class DeduplicateStateSpec extends AnyFlatSpec with Matchers {
  behavior of classOf[DeduplicationState].getSimpleName

  private val t0 = Time.Timestamp.now()
  private val t1 = t0.add(Duration.ofMinutes(1L))
  private val t2 = t0.add(Duration.ofMinutes(2L))
  private val t3 = t0.add(Duration.ofMinutes(3L))

  private val bridgeMetrics = new BridgeMetrics(new Metrics(new MetricRegistry))

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
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(changeId(1) -> t0)
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(2L),
        recordTime = t1,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(changeId(1) -> t0)
        isDuplicate shouldBe true
      }
      ._1
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(2L),
        recordTime = t3,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
          changeId(1) -> t3
        )
        isDuplicate shouldBe false
      }
  }

  it should "evicts old entries (older than max deduplication time)" in {
    val deduplicationState = DeduplicationState.empty(
      deduplicationDuration = Duration.ofMinutes(2L),
      bridgeMetrics = bridgeMetrics,
    )

    deduplicationState
      .deduplicate(
        changeId = changeId(1),
        commandDeduplicationDuration = Duration.ofMinutes(1L),
        recordTime = t0,
      )
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
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
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
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
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
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
        ex.getMessage shouldBe s"Cannot deduplicate for a period ($commandDeduplicationDuration) longer than the max deduplication duration ($maxDeduplicationDuration)."
      case Success(_) => fail("It should throw an exception on invalid deduplication durations")
    }
  }

  private def changeId(idx: Int): ChangeId = ChangeId(
    applicationId = Ref.ApplicationId.assertFromString("some-app"),
    commandId = Ref.CommandId.assertFromString(s"some-command-$idx"),
    actAs = Set.empty,
  )
}
