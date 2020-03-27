// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.DurationInt

class AkkaQueueBasedObservedTimeServiceBackendSpec
    extends AsyncWordSpec
    with AsyncTimeLimitedTests
    with Matchers
    with AkkaBeforeAndAfterAll {

  override def timeLimit: Span = 10.seconds

  "an Akka queue-based observed time service backend" should {
    "return the time it started with" in {
      val timeService =
        TimeServiceBackend.observing(TimeServiceBackend.simple(instantAt(month = 1)))
      timeService.changes.use { changes =>
        timeService.getCurrentTime should be(instantAt(month = 1))
        changes.take(1).runWith(Sink.seq).map {
          _ should be(Seq(instantAt(month = 1)))
        }
      }
    }

    "update the time to a new time" in {
      val timeService =
        TimeServiceBackend.observing(TimeServiceBackend.simple(instantAt(month = 1)))
      timeService.changes.use { changes =>
        for {
          _ <- timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))
          completeChanges <- changes.take(2).runWith(Sink.seq)
        } yield {
          timeService.getCurrentTime should be(instantAt(month = 2))
          completeChanges should be(Seq(instantAt(month = 1), instantAt(month = 2)))
        }
      }
    }

    "support multiple change listeners" in {
      val timeService =
        TimeServiceBackend.observing(TimeServiceBackend.simple(instantAt(month = 1)))
      timeService.changes.use { changesA =>
        for {
          _ <- timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))
          _ = timeService.getCurrentTime should be(instantAt(month = 2))
          firstSetOfChangesA <- changesA.take(2).runWith(Sink.seq)
          _ = firstSetOfChangesA should be(Seq(instantAt(month = 1), instantAt(month = 2)))
          result <- timeService.changes.use { changesB =>
            for {
              _ <- timeService.setCurrentTime(instantAt(month = 2), instantAt(month = 3))
              _ = timeService.getCurrentTime should be(instantAt(month = 3))
              secondSetOfChangesA <- changesA.take(1).runWith(Sink.seq)
              completeChangesB <- changesB.take(2).runWith(Sink.seq)
            } yield {
              secondSetOfChangesA should be(Seq(instantAt(month = 3)))
              completeChangesB should be(Seq(instantAt(month = 2), instantAt(month = 3)))
            }
          }
        } yield result
      }
    }

    "stop observing when the resource is released" in {
      val timeService =
        TimeServiceBackend.observing(TimeServiceBackend.simple(instantAt(month = 1)))
      for {
        changes <- timeService.changes.use { changes =>
          for {
            _ <- timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))
          } yield changes
        }
        _ <- timeService.setCurrentTime(instantAt(month = 2), instantAt(month = 3))
        completeChanges <- changes.take(2).runWith(Sink.seq)
      } yield {
        timeService.getCurrentTime should be(instantAt(month = 3))
        completeChanges should be(Seq(instantAt(month = 1), instantAt(month = 2)))
      }
    }

    "not emit rejected changes" in {
      val timeService =
        TimeServiceBackend.observing(TimeServiceBackend.simple(instantAt(month = 1)))
      for {
        changes <- timeService.changes.use { changes =>
          for {
            _ <- timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 2))
            _ <- timeService.setCurrentTime(instantAt(month = 1), instantAt(month = 3))
          } yield changes
        }
        completeChanges <- changes.runWith(Sink.seq)
      } yield {
        timeService.getCurrentTime should be(instantAt(month = 2))
        completeChanges should be(Seq(instantAt(month = 1), instantAt(month = 2)))
      }
    }
  }

  // always construct new instants to avoid sharing references, which would allow us to cheat when
  // comparing them inside the SimpleTimeServiceBackend
  private def instantAt(month: Int): Instant =
    ZonedDateTime.of(2020, month, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
}
