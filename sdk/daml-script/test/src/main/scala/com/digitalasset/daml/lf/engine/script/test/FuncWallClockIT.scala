// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import java.time.Duration
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.value.Value._

class FuncWallClockIT extends AbstractFuncIT {
  protected override lazy val timeMode = ScriptTimeMode.WallClock

  "testSleep" should {
    "sleep for specified duration" in {
      for {
        clients <- scriptClients()
        ValueRecord(_, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTestWithKeys:sleepTest"),
          dar = dar,
        )
      } yield {
        assert(vals.length == 3)
        val t0 = assertValueTimestamp(vals(0)._2).toInstant
        val t1 = assertValueTimestamp(vals(1)._2).toInstant
        val t2 = assertValueTimestamp(vals(2)._2).toInstant

        val duration1 = Duration.between(t0, t1)
        val duration2 = Duration.between(t1, t2)

        // Sleep uses the monotonic clock (System.nanoTime) for accurate real-time
        // duration, but getTime uses the wall clock (Clock.systemUTC). These clocks
        // can drift slightly due to NTP adjustments, so we allow a small tolerance.
        val clockDriftTolerance = Duration.ofMillis(5)

        val required1 = Duration.ofMillis(1000).minus(clockDriftTolerance)
        val required2 = Duration.ofMillis(2000).minus(clockDriftTolerance)
        val required1_upper = Duration.ofMillis(1100)
        val required2_upper = Duration.ofMillis(2100)

        duration1 should be >= required1
        duration1 should be < required1_upper

        duration2 should be >= required2
        duration2 should be < required2_upper
      }
    }
  }
}
