// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.TimeProofRequestConfig
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  Deliver,
  MessageId,
  SignedContent,
  TimeProof,
}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{Future, Promise}

class TimeProofRequestSubmitterTest extends FixtureAsyncWordSpec with BaseTest {

  class Env extends AutoCloseable {
    val config = TimeProofRequestConfig()
    val callCount = new AtomicInteger()
    val nextRequestP = new AtomicReference[Option[Promise[Unit]]](None)
    val nextResult =
      new AtomicReference[EitherT[Future, SendAsyncClientError, Unit]](
        EitherTUtil.unit[SendAsyncClientError]
      )
    val clock = new SimClock(loggerFactory = loggerFactory)
    val timeRequestSubmitter =
      new TimeProofRequestSubmitterImpl(config, handleRequest, clock, timeouts, loggerFactory)

    private def handleRequest(
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SendAsyncClientError, Unit] = {
      callCount.incrementAndGet()
      nextRequestP.get.foreach(_.trySuccess(()))
      nextRequestP.set(None)
      nextResult.get().mapK(FutureUnlessShutdown.outcomeK)
    }

    def triggerTime(): EitherT[Future, SendAsyncClientError, Unit] = {
      callCount.incrementAndGet()
      nextResult.get()
    }

    def waitForNextRequest(): Future[Unit] = {
      val promise = Promise[Unit]()
      if (!nextRequestP.compareAndSet(None, promise.some)) {
        fail("promise for next request was already setup")
      }
      promise.future
    }

    def setupNextResultPromise(): Promise[Either[SendAsyncClientError, Unit]] = {
      val promise = Promise[Either[SendAsyncClientError, Unit]]()

      nextResult.set(EitherT(promise.future))

      promise
    }

    def mkTimeProof(seconds: Int): TimeProof = {
      val event =
        OrdinarySequencedEvent(
          SignedContent(
            Deliver.create(
              SequencerCounter(0),
              CantonTimestamp.ofEpochSecond(seconds.toLong),
              DefaultTestIdentities.domainId,
              Some(MessageId(String73(s"tick-$seconds")())),
              Batch.empty(testedProtocolVersion),
              None,
              testedProtocolVersion,
              Option.empty[TrafficReceipt],
            ),
            SymbolicCrypto.emptySignature,
            None,
            testedProtocolVersion,
          )
        )(traceContext)
      TimeProof.fromEventO(event).value
    }

    override def close(): Unit = timeRequestSubmitter.close()
  }

  type FixtureParam = Env

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {
      env.close()
    }
  }

  "time request submitter" should {
    "avoid making concurrent calls when a request is in progress" in { env =>
      import env.*

      val nextRequestP = waitForNextRequest()
      // should trigger a request
      timeRequestSubmitter.fetchTimeProof()
      // should now just reuse the pending prior request
      timeRequestSubmitter.fetchTimeProof()
      timeRequestSubmitter.handleTimeProof(mkTimeProof(0))

      for {
        _ <- nextRequestP
      } yield callCount.get() shouldBe 1
    }

    "retry request if an appropriate event is not witnessed within our custom max-sequencing-duration" in {
      env =>
        import env.*

        val request1F = waitForNextRequest()
        timeRequestSubmitter.fetchTimeProof() // kicks off getting a time event

        for {
          _ <- timeRequestSubmitter.flush()
          _ <- request1F
          // setup waiting for the next request but don't yet wait for it
          request2F = waitForNextRequest()
          // now advance past the time that we think we should have seen the time event
          _ = clock.advance(config.maxSequencingDelay.asJava)
          // now expect that a new request is made
          _ <- timeRequestSubmitter.flush()
          _ <- request2F
          // if a time event is now witnessed we don't make another request
          _ = timeRequestSubmitter.handleTimeProof(mkTimeProof(0))
          _ = clock.advance(config.maxSequencingDelay.asJava.plusMillis(1))
          _ <- timeRequestSubmitter.flush()
        } yield callCount.get() shouldBe 2
    }

    "avoid more than one pending request when a time event is witnessed and new request started during the max-sequencing duration" in {
      env =>
        import env.*

        timeRequestSubmitter.fetchTimeProof()
        val callCountAtStart = callCount.get()

        for {
          _ <- timeRequestSubmitter.flush()
          // immediately witness an event
          _ = timeRequestSubmitter.handleTimeProof(mkTimeProof(0))
          _ <- timeRequestSubmitter.flush()
          callCountNoRequest = callCount.get()
          // then immediately start a new request
          request2F = waitForNextRequest()
          _ = timeRequestSubmitter.fetchTimeProof()
          // that should have started a new request
          _ <- timeRequestSubmitter.flush()
          _ <- request2F
          callCountBefore = callCount.get()
          // now when the maxSequencingDelay has elapsed we'll have two scheduled checks looking to see if they should attempt another request
          // the first has actually already been satisfied but the second is still pending
          // we want to see only a single request being made as the first request should determine it's no longer active
          _ = clock.advance(config.maxSequencingDelay.asJava)
          _ <- timeRequestSubmitter.flush()
          callCountAfter = callCount.get()
        } yield {
          callCountNoRequest shouldBe callCountAtStart
          callCountAfter shouldBe (callCountBefore + 1)
        }
    }
  }
}
