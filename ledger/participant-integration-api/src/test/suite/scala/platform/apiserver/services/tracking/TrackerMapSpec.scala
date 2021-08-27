// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.tracking

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.daml.ledger.api.v1.commands.Commands
import com.daml.ledger.client.services.commands.CommandSubmission
import com.daml.ledger.client.services.commands.tracker.CompletionResponse.{
  CompletionSuccess,
  TrackedCompletionFailure,
}
import com.daml.logging.LoggingContext
import com.daml.platform.apiserver.services.tracking.TrackerMapSpec._
import com.daml.timer.Delayed
import com.google.rpc.status.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.compat._
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{ExecutionContext, Future}

class TrackerMapSpec extends AsyncWordSpec with Matchers {
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "the tracker map" should {
    "delegate submissions to the constructed trackers" in {
      val transactionIds = Seq("transaction A", "transaction B").iterator
      val tracker = new TrackerMap[String](
        retentionPeriod = 1.minute,
        getKey = commands => commands.commandId,
        newTracker = _ => Future.successful(new FakeTracker(transactionIds)),
      )

      for {
        completion1 <- tracker.track(
          CommandSubmission(Commands(commandId = "1", actAs = Seq("Alice")))
        )
        completion2 <- tracker.track(
          CommandSubmission(Commands(commandId = "2", actAs = Seq("Bob")))
        )
      } yield {
        completion1 should matchPattern { case Right(CompletionSuccess("1", "transaction A", _)) =>
        }
        completion2 should matchPattern { case Right(CompletionSuccess("2", "transaction B", _)) =>
        }
      }
    }

    "only construct one tracker per key" in {
      val trackerCount = new AtomicInteger(0)
      val tracker = new TrackerMap[Set[String]](
        retentionPeriod = 1.minute,
        getKey = commands => commands.actAs.toSet,
        newTracker = actAs => {
          trackerCount.incrementAndGet()
          val transactionIds =
            (0 until Int.MaxValue).iterator.map(i => s"${actAs.mkString(", ")}: $i")
          Future.successful(new FakeTracker(transactionIds))
        },
      )

      for {
        completion1 <- tracker.track(
          CommandSubmission(Commands(commandId = "X", actAs = Seq("Alice", "Bob")))
        )
        completion2 <- tracker.track(
          CommandSubmission(Commands(commandId = "Y", actAs = Seq("Bob", "Alice")))
        )
      } yield {
        trackerCount.get() should be(1)
        completion1 should matchPattern { case Right(CompletionSuccess("X", "Alice, Bob: 0", _)) =>
        }
        completion2 should matchPattern { case Right(CompletionSuccess("Y", "Alice, Bob: 1", _)) =>
        }
      }
    }

    "only construct one tracker per key, even under heavy contention" in {
      val requestCount = 1000
      val expectedTrackerCount = 10
      val actualTrackerCount = new AtomicInteger(0)
      val tracker = new TrackerMap[String](
        retentionPeriod = 1.minute,
        getKey = commands => commands.applicationId,
        newTracker = _ => {
          actualTrackerCount.incrementAndGet()
          Future.successful(new FakeTracker(transactionIds = Iterator.continually("")))
        },
      )

      val requests = (0 until requestCount).map { i =>
        val key = (i % expectedTrackerCount).toString
        CommandSubmission(Commands(commandId = i.toString, applicationId = key))
      }
      Future.sequence(requests.map(tracker.track)).map { completions =>
        actualTrackerCount.get() should be(expectedTrackerCount)
        all(completions) should matchPattern { case Right(_) => }
      }
    }

    "clean up old trackers" in {
      val trackerCounts = TrieMap.empty[Set[String], AtomicInteger]
      val tracker = new TrackerMap[Set[String]](
        retentionPeriod = Duration.Zero,
        getKey = commands => commands.actAs.toSet,
        newTracker = actAs => {
          trackerCounts.getOrElseUpdate(actAs, new AtomicInteger(0)).incrementAndGet()
          Future.successful(new FakeTracker(transactionIds = Iterator.continually("")))
        },
      )

      for {
        _ <- tracker.track(CommandSubmission(Commands(commandId = "1", actAs = Seq("Alice"))))
        _ <- tracker.track(CommandSubmission(Commands(commandId = "2", actAs = Seq("Bob"))))
        _ = tracker.cleanup()
        _ <- tracker.track(CommandSubmission(Commands(commandId = "3", actAs = Seq("Bob"))))
      } yield {
        val finalTrackerCounts = trackerCounts.view.mapValues(_.get()).toMap
        finalTrackerCounts should be(Map(Set("Alice") -> 1, Set("Bob") -> 2))
      }
    }

    "clean up failed trackers" in {
      val trackerCounts = TrieMap.empty[String, AtomicInteger]
      val tracker = new TrackerMap[String](
        retentionPeriod = 1.minute,
        getKey = commands => commands.applicationId,
        newTracker = applicationId => {
          trackerCounts.getOrElseUpdate(applicationId, new AtomicInteger(0)).incrementAndGet()
          if (applicationId.isEmpty)
            Future.failed(new IllegalArgumentException("Missing application ID."))
          else
            Future.successful(new FakeTracker(transactionIds = Iterator.continually("")))
        },
      )

      for {
        _ <- tracker.track(CommandSubmission(Commands(commandId = "1", applicationId = "test")))
        failure1 <- tracker
          .track(CommandSubmission(Commands(commandId = "2", applicationId = "")))
          .failed
        _ = tracker.cleanup()
        failure2 <- tracker
          .track(CommandSubmission(Commands(commandId = "3", applicationId = "")))
          .failed
      } yield {
        val finalTrackerCounts = trackerCounts.view.mapValues(_.get()).toMap
        finalTrackerCounts should be(Map("test" -> 1, "" -> 2))
        failure1.getMessage should be("Missing application ID.")
        failure2.getMessage should be("Missing application ID.")
      }
    }

    "close all trackers" in {
      val requestCount = 20
      val expectedTrackerCount = 5
      val openTrackerCount = new AtomicInteger(0)
      val closedTrackerCount = new AtomicInteger(0)
      val tracker = new TrackerMap[String](
        retentionPeriod = 1.minute,
        getKey = commands => commands.applicationId,
        newTracker = _ =>
          Future.successful {
            openTrackerCount.incrementAndGet()
            new Tracker {
              override def track(
                  submission: CommandSubmission
              )(implicit
                  executionContext: ExecutionContext,
                  loggingContext: LoggingContext,
              ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
                Future.successful(
                  Right(
                    CompletionSuccess(
                      commandId = submission.commands.commandId,
                      transactionId = "",
                      originalStatus = Status.defaultInstance,
                    )
                  )
                )

              override def close(): Unit = {
                closedTrackerCount.incrementAndGet()
                ()
              }
            }
          },
      )

      val requests = (0 until requestCount).map { i =>
        val key = (i % expectedTrackerCount).toString
        CommandSubmission(Commands(commandId = i.toString, applicationId = key))
      }
      for {
        _ <- Future.sequence(requests.map(tracker.track))
        _ = tracker.close()
      } yield {
        openTrackerCount.get() should be(expectedTrackerCount)
        closedTrackerCount.get() should be(expectedTrackerCount)
      }
    }

    "close waiting trackers" in {
      val openTracker = new AtomicBoolean(false)
      val closedTracker = new AtomicBoolean(false)
      val tracker = new TrackerMap[Unit](
        retentionPeriod = 1.minute,
        getKey = _ => (),
        newTracker = _ =>
          Delayed.by(1.second) {
            openTracker.set(true)
            new Tracker {
              override def track(
                  submission: CommandSubmission
              )(implicit
                  executionContext: ExecutionContext,
                  loggingContext: LoggingContext,
              ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
                Future.successful(
                  Right(
                    CompletionSuccess(
                      commandId = submission.commands.commandId,
                      transactionId = "",
                      originalStatus = Status.defaultInstance,
                    )
                  )
                )

              override def close(): Unit = {
                closedTracker.set(true)
                ()
              }
            }
          },
      )

      val completionF = tracker.track(CommandSubmission(Commands(commandId = "command")))
      tracker.close()
      Delayed.Future.by(1.second)(completionF).map { completion =>
        openTracker.get() should be(true)
        closedTracker.get() should be(true)
        completion should matchPattern { case Right(_) => }
      }
    }
  }
}

object TrackerMapSpec {
  final class FakeTracker(transactionIds: Iterator[String]) extends Tracker {
    override def track(
        submission: CommandSubmission
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[Either[TrackedCompletionFailure, CompletionSuccess]] =
      Future.successful(
        Right(
          CompletionSuccess(
            commandId = submission.commands.commandId,
            transactionId = transactionIds.next(),
            originalStatus = Status.defaultInstance,
          )
        )
      )

    override def close(): Unit = ()
  }
}
