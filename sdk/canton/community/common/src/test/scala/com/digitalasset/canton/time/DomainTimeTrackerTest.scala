// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import cats.syntax.option.*
import com.digitalasset.canton.config.DomainTimeTrackerConfig
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.sequencing.OrdinaryProtocolEvent
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, MessageId, SignedContent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, SequencerCounter, config}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}

class MockTimeRequestSubmitter extends TimeProofRequestSubmitter {
  private val hasRequestedRef = new AtomicBoolean(false)

  def hasRequestedTime: Boolean = hasRequestedRef.get()

  def resetHasRequestedTime(): Unit = hasRequestedRef.set(false)

  val fetchResult = Promise[TimeProof]()

  override def fetchTimeProof()(implicit traceContext: TraceContext): Unit = {
    hasRequestedRef.set(true)
  }

  override def handleTimeProof(proof: TimeProof): Unit = ()

  override def close(): Unit = ()
}

class DomainTimeTrackerTest extends FixtureAsyncWordSpec with BaseTest {
  def ts(epochSeconds: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(epochSeconds.toLong)

  def timeProofEvent(ts: CantonTimestamp): OrdinaryProtocolEvent =
    OrdinarySequencedEvent(
      SignedContent(
        Deliver.create(
          SequencerCounter(0),
          ts,
          DefaultTestIdentities.domainId,
          TimeProof.mkTimeProofRequestMessageId.some,
          Batch.empty(testedProtocolVersion),
          testedProtocolVersion,
        ),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      ),
      None,
    )(traceContext)

  def otherEvent(ts: CantonTimestamp): OrdinaryProtocolEvent = {
    // create a event which won't be flagged as a time proof
    val event = OrdinarySequencedEvent(
      SignedContent(
        Deliver.create(
          SequencerCounter(0),
          ts,
          DefaultTestIdentities.domainId,
          MessageId.tryCreate("not a time proof").some,
          Batch.empty(testedProtocolVersion),
          testedProtocolVersion,
        ),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      ),
      None,
    )(traceContext)

    // make sure future changes don't treat this as a time proof
    TimeProof.fromEventO(event) shouldBe None

    event
  }

  class Env {
    // allow 2s to see events for a timestamp from our local clock
    val observationLatencySecs = 2
    // put off requesting a time if we've seen an event within the last 4s
    val patienceDurationSecs = 4
    val domainTimeTrackerConfig = DomainTimeTrackerConfig(
      config.NonNegativeFiniteDuration.ofSeconds(observationLatencySecs.toLong),
      config.NonNegativeFiniteDuration.ofSeconds(patienceDurationSecs.toLong),
    )
    val clock = new SimClock(loggerFactory = loggerFactory)
    val requestSubmitter = new MockTimeRequestSubmitter
    val timeTracker =
      new DomainTimeTracker(
        domainTimeTrackerConfig,
        clock,
        requestSubmitter,
        timeouts,
        loggerFactory,
      )

    def observeTimeProof(epochSecs: Int): Future[Unit] =
      Future.successful(timeTracker.update(Seq(timeProofEvent(ts(epochSecs)))))

    def observeTimestamp(epochSecs: Int): Future[Unit] =
      Future.successful(timeTracker.update(Seq(otherEvent(ts(epochSecs)))))

    def advanceTo(epochSeconds: Int): Future[Unit] = {
      clock.advanceTo(ts(epochSeconds))
      Future.unit
    }

    def advanceToAndFlush(epochSecs: Int): Future[Unit] = {
      clock.advanceTo(ts(epochSecs))
      timeTracker.flush()
    }

    def advanceAndFlush(secs: Int): Future[Unit] = {
      clock.advance(Duration.ofSeconds(secs.toLong))
      timeTracker.flush()
    }
  }

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new Env

    withFixture(test.toNoArgAsyncTest(env))
  }

  override type FixtureParam = Env

  "requestTick" should {
    // keep waiting if we're seeing regular events from the domain

    "do nothing if a event is witnessed with an appropriate tick" in { env =>
      import env.*

      timeTracker.requestTick(ts(2))

      for {
        _ <- advanceTo(2)
        // shouldn't have asked for a time proof as we're within the observation latency
        _ = requestSubmitter.hasRequestedTime shouldBe false
        _ <- advanceTo(3)
        // shouldn't have asked for a time as despite our local clock being ahead of what we want to witness,
        // we're still behind that plus the observation latency
        _ = requestSubmitter.hasRequestedTime shouldBe false
        // now we'll produce an event which is past the time we're looking for
        _ <- observeTimeProof(3)
        // now we'll zoom ahead and make sure we never request a time proof as we don't need one
        _ <- advanceTo(100)
      } yield requestSubmitter.hasRequestedTime shouldBe false
    }

    "request time proof if we surpass the time we're expecting" in { env =>
      import env.*

      timeTracker.subscriptionResumesAfter(ts(1))
      timeTracker.requestTick(ts(2))

      for {
        _ <- advanceTo(2)
        _ = requestSubmitter.hasRequestedTime shouldBe false
        _ <- advanceAndFlush(observationLatencySecs)
      } yield requestSubmitter.hasRequestedTime shouldBe true
    }

    "request time proof only after we have received the first event" in { env =>
      import env.*

      timeTracker.requestTick(ts(2))
      for {
        _ <- advanceTo(2)
        _ = requestSubmitter.hasRequestedTime shouldBe false
        _ = clock.advance(Duration.ofSeconds(observationLatencySecs.toLong))
        _ = requestSubmitter.hasRequestedTime shouldBe false
        _ = observeTimestamp(1)
        _ = advanceAndFlush(patienceDurationSecs)
      } yield requestSubmitter.hasRequestedTime shouldBe true
    }

    "request time proof immediately" in { env =>
      import env.*

      timeTracker.requestTick(ts(2), immediately = true)
      requestSubmitter.hasRequestedTime shouldBe true
    }

    "ignore requested tick if too large to track" in { env =>
      import env.*

      // as we wait for the observation latency after the requested domain time using max value
      // would cause the timestamp we're looking for to overflow
      loggerFactory.assertLogs(
        timeTracker.requestTicks(
          Seq(
            CantonTimestamp.MaxValue,
            CantonTimestamp.MaxValue.minusSeconds(2),
            CantonTimestamp.MaxValue.minusSeconds(1),
          )
        ),
        _.warningMessage should (include(
          s"Ignoring request for 3 ticks from ${CantonTimestamp.MaxValue.minusSeconds(2)} to ${CantonTimestamp.MaxValue} as they are too large"
        )),
      )
      timeTracker.earliestExpectedObservationTime() shouldBe None

      // the upper bound is the time - observationLatency
      loggerFactory.assertLogs(
        timeTracker.requestTick(
          CantonTimestamp.MaxValue.minus(domainTimeTrackerConfig.observationLatency.asJava)
        ),
        _.warningMessage should (include("Ignoring request for 1 ticks") and include(
          "as they are too large"
        )),
      )
      timeTracker.earliestExpectedObservationTime() shouldBe None

      // but slightly below that should be suitable for tracking (despite being practically useless given it's in 9999)
      loggerFactory.assertLogs(
        timeTracker.requestTicks(
          Seq(
            CantonTimestamp.MaxValue,
            CantonTimestamp.MaxValue
              .minus(domainTimeTrackerConfig.observationLatency.asJava)
              .immediatePredecessor,
          )
        ),
        _.warningMessage should (include("Ignoring request for 1 ticks") and include(
          "as they are too large"
        )),
      )
      timeTracker.earliestExpectedObservationTime().isDefined shouldBe true
    }
  }

  "fetch" should {
    "timestamp should resolve on any received event" in { env =>
      import env.*

      timeTracker.subscriptionResumesAfter(ts(0))
      clock.advance(Duration.ofSeconds(patienceDurationSecs.toLong))

      // make two distinct requests for the next timestamp to ensure they will all be resolved by the same event
      val fetchP1 = timeTracker.fetchTime()
      val fetchP2 = timeTracker.fetchTime()

      // should have immediately requested a fresh timestamp
      requestSubmitter.hasRequestedTime shouldBe true

      for {
        // provide an event with a timestamp (not our response to requesting a time)
        _ <- observeTimeProof(42)
        fetch1 <- fetchP1.failOnShutdown("fetch first time proof")
        fetch2 <- fetchP2.failOnShutdown("fetch second time proof")
      } yield {
        fetch1 shouldBe ts(42)
        fetch2 shouldBe ts(42)
      }
    }

    "immediately return if we have a suitably fresh timestamp" in { env =>
      import env.*

      clock.advanceTo(ts(1))

      for {
        _ <- observeTimeProof(42)
        _ = clock.advanceTo(ts(5))
        // should return the existing observation as it's within the freshness bounds
        fetch1 <- timeTracker
          .fetchTime(NonNegativeFiniteDuration.tryOfSeconds(5))
          .failOnShutdown("fetch time")
        _ = fetch1 shouldBe ts(42)
        // we've returned a sufficiently fresh time without causing a request
        _ = requestSubmitter.hasRequestedTime shouldBe false
        // however if we now request a timestamp that was received within the last 2 seconds, we'll have to go fetch one
        fetch2F = timeTracker
          .fetchTime(NonNegativeFiniteDuration.tryOfSeconds(2))
          .failOnShutdown("fetch time")
        _ = requestSubmitter.hasRequestedTime shouldBe true
        _ <- observeTimeProof(43)
        fetch2 <- fetch2F
        // should now hand us the new observation
      } yield fetch2 shouldBe ts(43)
    }

    "fetching time proof when there isn't a fresh one available should immediately force request" in {
      env =>
        import env.*

        clock.advanceTo(ts(1))

        for {
          // observe a recent event which isn't a time proof
          _ <- observeTimestamp(1)
          timeProofF = timeTracker.fetchTimeProof().failOnShutdown("fetch time")
          // also we've seen a recent event we know this won't suffice for a time proof
          _ = requestSubmitter.hasRequestedTime shouldBe true
          // then observe one
          _ <- observeTimeProof(42)
          timeProof <- timeProofF
        } yield timeProof.timestamp shouldBe ts(42)
    }

    "stop waiting on shutdown" in { env =>
      import env.*

      clock.advanceTo(ts(1))

      for {
        // observe a recent event which isn't a time proof
        _ <- observeTimestamp(1)
        timeProofF = timeTracker.fetchTimeProof()
        // Shutdown the time tracker
        _ = timeTracker.close()
        timeProof <- timeProofF.unwrap
      } yield timeProof shouldBe AbortedDueToShutdown
    }
  }

  "awaitTick" should {
    "only resolve future when we've reached the given time" in { env =>
      import env.*

      for {
        _ <- observeTimeProof(1)
        awaitO = timeTracker.awaitTick(ts(3))
        awaitF =
          awaitO.value // should have returned a future as we have not yet observed the requested domain time
        _ <- observeTimeProof(2)
        _ = awaitF.isCompleted shouldBe false
        _ <- observeTimeProof(3)
        awaitedTs <- awaitF
      } yield awaitedTs shouldBe ts(3)
    }

    "return None if we've already witnessed an equal or greater timestamp from the domain" in {
      env =>
        import env.*

        for {
          _ <- observeTimeProof(42)
          awaitO = timeTracker.awaitTick(ts(10))
        } yield {
          awaitO shouldBe None
          timeTracker.earliestExpectedObservationTime() shouldBe None
        }
    }
  }

  "ensure minimum time interval" should {
    "should ask for time if a sufficient amount of local time progresses" in { env =>
      import env.*

      timeTracker.subscriptionResumesAfter(ts(0))
      // advance to our min observation duration without witnessing a time
      clock.advance(domainTimeTrackerConfig.minObservationDuration.asJava.plusMillis(1))
      // we should request one
      requestSubmitter.hasRequestedTime shouldBe true
      requestSubmitter.resetHasRequestedTime() // reset to use again

      for {
        // will resolve our request to fetch a time
        _ <- observeTimeProof(10)
        // advance to almost the time we should wait
        _ = clock.advance(domainTimeTrackerConfig.minObservationDuration.asJava.minusSeconds(1))
        _ <- observeTimeProof(11) // observe a time
        _ = clock.advance(domainTimeTrackerConfig.minObservationDuration.asJava.minusSeconds(1))
        _ = requestSubmitter.hasRequestedTime shouldBe false // we shouldn't have requested a time
        _ = clock.advance(
          Duration.ofSeconds(1).plusMillis(1)
        ) // advance one more second and a bit to hit our window
      } yield requestSubmitter.hasRequestedTime shouldBe true // should now have requested
    }
  }
}
