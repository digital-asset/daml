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
        val t0 = assertSTimestamp(vals(0))
        val t1 = assertSTimestamp(vals(1))
        val t2 = assertSTimestamp(vals(2))
        assert(Duration.between(t0.toInstant, t1.toInstant).compareTo(Duration.ofSeconds(1)) >= 0)
        assert(Duration.between(t1.toInstant, t2.toInstant).compareTo(Duration.ofSeconds(2)) >= 0)
      }
    }
  }
}
