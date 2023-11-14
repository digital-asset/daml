// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream.scaladsl.{Keep, Sink, SinkQueueWithCancel}
import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberId
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.{Member, ParticipantId}
import com.digitalasset.canton.util.AkkaUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.scalatest.{Assertion, FutureOutcome}

import java.util.concurrent.atomic.AtomicLong
import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration.*

class LocalSequencerStateEventSignallerTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  val alice: Member = ParticipantId("alice")
  val bob: Member = ParticipantId("bob")
  val aliceId = SequencerMemberId(0)
  val bobId = SequencerMemberId(1)

  class Env extends FlagCloseable with NamedLogging {
    override val timeouts = LocalSequencerStateEventSignallerTest.this.timeouts
    protected override val loggerFactory = LocalSequencerStateEventSignallerTest.this.loggerFactory
    implicit val actorSystem =
      AkkaUtil.createActorSystem(loggerFactory.threadName)(parallelExecutionContext)

    val nextTimestampSecond = new AtomicLong(0L)
    def generateTimestamp(): CantonTimestamp =
      CantonTimestamp.ofEpochSecond(nextTimestampSecond.getAndIncrement())
    // generate a few upfront
    val ts0 = generateTimestamp()
    val ts1 = generateTimestamp()
    val ts2 = generateTimestamp()

    val signaller = new LocalSequencerStateEventSignaller(timeouts, loggerFactory)

    def expectSignal(queue: SinkQueueWithCancel[ReadSignal]): Future[Assertion] =
      queue.pull() map {
        case None => fail("queue completed without a signal")
        case Some(_) => succeed
      }

    def expectComplete(queue: SinkQueueWithCancel[ReadSignal]): Future[Assertion] =
      queue.pull() map {
        case None => succeed
        case Some(_) => fail("received item but expected complete")
      }

    override protected def onClosed(): Unit =
      Lifecycle.close(
        signaller,
        Lifecycle.toCloseableActorSystem(actorSystem, logger, timeouts),
      )(logger)
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }
  override type FixtureParam = Env

  "writer updates without subscribers don't block writer" in { env =>
    import env.*

    for {
      // write a number that will certainly exceed any local buffers the akka stream operators may have
      // the test here is just checking it doesn't deadlock
      _ <- (0 until 3001).toList.parTraverse(_ =>
        signaller.notifyOfLocalWrite(WriteNotification.Members(SortedSet(aliceId)))
      )
      // regardless of all of the prior events that were pummeled only a single signal is produced when subscribed
      // what's past is prologue
      aliceSignals <- AkkaUtil.runSupervised(
        logger.error("writer updates", _),
        signaller
          .readSignalsForMember(alice, aliceId)
          .takeWithin(
            200.millis
          ) // crude wait to receive more signals if some were going to be produced
          .toMat(Sink.seq)(Keep.right),
      )
    } yield {
      aliceSignals should have size (2) // there is a one item buffer on both the source queue and broadcast hub
    }
  }
}
