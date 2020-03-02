// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandboxnext

import java.time.{Clock, Instant}

import akka.stream.scaladsl.Sink
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.duration.{Duration, DurationInt}

class RegularHeartbeatSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll {
  "a regular heartbeat" should {
    "poll the clock" in {
      val startTime = Instant.now()
      val expectedTimestamps = (1L to 10L).map(startTime.plusSeconds)
      val clock = mock[Clock]
      when(clock.instant()).thenReturn(expectedTimestamps.head, expectedTimestamps.tail: _*)

      val heartbeat = new RegularHeartbeat(clock, Duration.Zero)
      heartbeat.use { heartbeats =>
        for {
          actualTimestamps <- heartbeats.take(10).runWith(Sink.seq)
        } yield {
          actualTimestamps should be(expectedTimestamps)
        }
      }
    }

    "beat regularly" in {
      val heartbeat = new RegularHeartbeat(Clock.systemUTC(), 1.second)
      heartbeat.use { heartbeats =>
        for {
          beats <- heartbeats.take(3).runWith(Sink.seq)
        } yield {
          val differences = beats
            .sliding(2)
            .map { case Seq(first, second) => (second.toEpochMilli - first.toEpochMilli).toInt }
            .toVector
          all(differences) should be(1000 +- 500)
        }
      }
    }
  }
}
