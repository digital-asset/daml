// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.ledger.participant.state.v2.ChangeId
import com.daml.lf.data.{Ref, Time}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import scala.collection.immutable.VectorMap
import scala.util.{Failure, Success, Try}
import scala.util.chaining._

class DeduplicateStateSpec extends AnyFlatSpec with Matchers {
  behavior of classOf[DeduplicationState].getSimpleName

  private val initialTime = Time.Timestamp.now()

  it should "deduplicate commands within the requested deduplication window" in {
    val deduplicationState = DeduplicationState.empty(
      deduplicationDuration = Duration.ofMinutes(3L),
      currentTime = currentTimeMock,
    )

    deduplicationState
      .deduplicate(changeId(1), Duration.ofMinutes(2L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(changeId(1) -> initialTime)
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(changeId(1), Duration.ofMinutes(2L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(changeId(1) -> initialTime)
        isDuplicate shouldBe true
      }
      ._1
      .deduplicate(changeId(1), Duration.ofMinutes(2L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
          changeId(1) -> initialTime.add(Duration.ofMinutes(2))
        )
        isDuplicate shouldBe false
      }
  }

  it should "evicts old entries (older than max deduplication time)" in {
    val deduplicationState = DeduplicationState.empty(
      deduplicationDuration = Duration.ofMinutes(2L),
      currentTime = currentTimeMock,
    )

    deduplicationState
      .deduplicate(changeId(1), Duration.ofMinutes(1L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
          changeId(1) -> initialTime
        )
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(changeId(2), Duration.ofMinutes(1L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
          changeId(1) -> initialTime,
          changeId(2) -> initialTime.add(Duration.ofMinutes(1)),
        )
        isDuplicate shouldBe false
      }
      ._1
      .deduplicate(changeId(3), Duration.ofMinutes(1L))
      .tap { case (newDeduplicationState, isDuplicate) =>
        newDeduplicationState.deduplicationQueue shouldBe VectorMap(
          changeId(2) -> initialTime.add(Duration.ofMinutes(1)),
          changeId(3) -> initialTime.add(Duration.ofMinutes(2)),
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
          currentTime = currentTimeMock,
        )
        .deduplicate(changeId(1337), commandDeduplicationDuration)
    ) match {
      case Failure(ex) =>
        ex.getMessage shouldBe s"Cannot deduplicate for a period ($commandDeduplicationDuration) longer than the max deduplication duration ($maxDeduplicationDuration)."
      case Success(_) => fail("It should throw an exception on invalid deduplication durations")
    }
  }

  // Current time provider mock builder.
  // On each call, the mock advances the time by 1 minute
  private def currentTimeMock: () => Time.Timestamp = {
    var currentTime = initialTime
    () => currentTime.tap(_ => currentTime = currentTime.add(Duration.ofMinutes(1L)))
  }

  private def changeId(idx: Int): ChangeId = ChangeId(
    applicationId = Ref.ApplicationId.assertFromString("some-app"),
    commandId = Ref.CommandId.assertFromString(s"some-command-$idx"),
    actAs = Set.empty,
  )
}
