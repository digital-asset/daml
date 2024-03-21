// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.daml.metrics.api.noop.NoOpGauge
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.FullSuppression
import com.digitalasset.canton.time.{NonNegativeFiniteDuration, SimClock}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class DelayLoggerTest extends AnyWordSpec with BaseTest {

  "checkForDelay" should {
    val clock = new SimClock(start = CantonTimestamp.now(), loggerFactory = loggerFactory)
    val gauge = NoOpGauge[Long]("test", 0L)
    val delayLogger =
      new DelayLogger(clock, logger, NonNegativeFiniteDuration.tryOfSeconds(1), gauge)

    def probe(delayMs: Long, assertion: Seq[LogEntry] => Assertion): Unit = {
      val event = mock[PossiblyIgnoredProtocolEvent]
      when(event.timestamp).thenReturn(clock.now.minusMillis(delayMs))
      loggerFactory.assertLogsSeq(FullSuppression)(
        delayLogger.checkForDelay(event),
        assertion,
      )
    }

    "not log when we haven't caught up yet" in {
      probe(2000, _ shouldBe empty)
    }
    "log success after we caught up" in {
      probe(500, forEvery(_) { _.message should include("Caught up") })
    }
    "log a warning if we are late again" in {
      probe(2000, forEvery(_) { _.warningMessage should include("Late batch") })
    }
    "don't log another warning if we are still late" in {
      probe(2000, _ shouldBe empty)
    }
    "log a notification if the situation is resolved" in {
      probe(500, forEvery(_) { _.message should include("Caught up") })
    }
    "not log another notification" in {
      probe(500, _ shouldBe empty)
    }
  }
}
