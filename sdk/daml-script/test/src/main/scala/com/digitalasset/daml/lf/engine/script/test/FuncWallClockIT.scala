// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
        start = System.nanoTime
        ValueRecord(_, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:sleepTest"),
          dar = dar,
        )
      } yield {
        val elapsed = Duration.ofNanos(System.nanoTime - start)
        elapsed should be >= Duration.ofMillis(1000 + 2000)

        assert(vals.length == 3)
        val t0 = assertValueTimestamp(vals(0)._2).toInstant
        val t1 = assertValueTimestamp(vals(1)._2).toInstant
        val t2 = assertValueTimestamp(vals(2)._2).toInstant

        val duration1 = Duration.between(t0, t1)
        val duration2 = Duration.between(t1, t2)

        duration1 should be < Duration.ofMillis(1100)
        duration2 should be < Duration.ofMillis(2100)
      }
    }
  }
}
