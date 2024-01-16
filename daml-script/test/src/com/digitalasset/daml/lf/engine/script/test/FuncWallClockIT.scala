// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.time.Duration
import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue.SRecord

class FuncWallClockITV1 extends FuncWallClockIT(LanguageMajorVersion.V1)

// TODO(https://github.com/digital-asset/daml/issues/17812): re-enable this test and control its run
//  at the bazel target level.
//class FuncWallClockITV2 extends FuncWallClockIT(LanguageMajorVersion.V2)

class FuncWallClockIT(override val majorLanguageVersion: LanguageMajorVersion)
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
        val t0 = assertSTimestamp(vals.get(0))
        val t1 = assertSTimestamp(vals.get(1))
        val t2 = assertSTimestamp(vals.get(2))
        assert(Duration.between(t0.toInstant, t1.toInstant).compareTo(Duration.ofSeconds(1)) >= 0)
        assert(Duration.between(t1.toInstant, t2.toInstant).compareTo(Duration.ofSeconds(2)) >= 0)
      }
    }
  }
}
