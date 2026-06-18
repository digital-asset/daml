// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import java.time.Duration
import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SValue.SRecord

class FuncWallClockITV2 extends FuncWallClockIT(LanguageVersion.Major.V2)

class FuncWallClockIT(override val majorLanguageVersion: LanguageVersion.Major)
    extends AbstractFuncIT {
  protected override lazy val timeMode = ScriptTimeMode.WallClock

  "testSleep" should {
    "sleep for specified duration" in {
      for {
        clients <- scriptClients()
        SRecord(_, _, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:sleepTest"),
          dar = dar,
        )
      } yield {
        assert(vals.size == 3)
        val t0 = assertSTimestamp(vals(0)).toInstant
        val t1 = assertSTimestamp(vals(1)).toInstant
        val t2 = assertSTimestamp(vals(2)).toInstant

        val duration1 = Duration.between(t0, t1)
        val duration2 = Duration.between(t1, t2)

        val required1 = Duration.ofMillis(1000)
        val required2 = Duration.ofMillis(2000)
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
