// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import cats.syntax.option.*
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.*

class PeriodicAcknowledgementsTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class Env(
      initialCleanTimestamp: Option[CantonTimestamp] = None,
      acknowledged: Promise[CantonTimestamp] = Promise(),
  ) extends AutoCloseable {
    val clock = new SimClock(loggerFactory = PeriodicAcknowledgementsTest.this.loggerFactory)
    var latestCleanTimestamp: Option[CantonTimestamp] = initialCleanTimestamp
    var nextResult: EitherT[FutureUnlessShutdown, String, Boolean] = EitherT.rightT(true)
    val acknowledgements = mutable.Buffer[CantonTimestamp]()
    val interval = 10.seconds

    val sut = new PeriodicAcknowledgements(
      true,
      interval,
      fetchLatestCleanTimestamp = _ => FutureUnlessShutdown.pure(latestCleanTimestamp),
      acknowledge = tts => {
        acknowledgements.append(tts.value)
        acknowledged.trySuccess(tts.value)
        nextResult
      },
      clock = clock,
      DefaultProcessingTimeouts.testing,
      PeriodicAcknowledgementsTest.this.loggerFactory,
    )

    override def close(): Unit = sut.close()
  }

  "should ack when first started" in {
    val ackedP = Promise[CantonTimestamp]()

    val _env @ unchecked =
      new Env(initialCleanTimestamp = CantonTimestamp.Epoch.some, acknowledged = ackedP)

    // it should pull the latest clean timestamp when created and acknowledge if present
    for {
      timestamp <- ackedP.future
    } yield timestamp shouldBe CantonTimestamp.Epoch
  }

  "should wait until the interval has elapsed before acknowledging again" in {
    val env = new Env()

    val ts0 = CantonTimestamp.Epoch
    val ts1 = CantonTimestamp.ofEpochSecond(5) // before 10s interval

    env.latestCleanTimestamp = ts0.some
    env.clock.advance(env.interval.plus(1.millis).toJava)

    for {
      _ <- env.sut.flush()
      _ = env.acknowledgements should contain.only(ts0)
      // set a new clean timestamp
      _ = env.latestCleanTimestamp = ts1.some
      // don't quite advance to the next interval
      _ = env.clock.advance(env.interval.minus(1.millis).toJava)
      _ <- env.sut.flush()
      _ = env.acknowledgements should contain.only(ts0)
      // advance past interval
      _ = env.clock.advance(1.millis.toJava)
      _ <- env.sut.flush()
    } yield env.acknowledgements should contain.only(ts0, ts1)
  }

  "should just log if acknowledging fails" in {
    val env = new Env()

    env.nextResult = EitherT.leftT("BOOM")
    env.latestCleanTimestamp = CantonTimestamp.Epoch.some

    for {
      _ <- loggerFactory.assertLogs(
        {
          env.clock.advance(env.interval.plus(1.millis).toJava)
          env.sut.flush()
        },
        _.warningMessage shouldBe "Failed to acknowledge clean timestamp (usually because sequencer is down): BOOM",
      )
    } yield succeed
  }
}
