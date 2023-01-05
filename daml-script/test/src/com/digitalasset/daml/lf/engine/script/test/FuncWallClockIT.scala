// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.time.Duration

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import com.daml.lf.speedy.SValue.SRecord

final class FuncWallClockIT extends AbstractFuncIT {
  override def timeMode = ScriptTimeMode.WallClock

  "testSleep" should {
    "sleep for specified duration" in {
      for {
        clients <- participantClients()
        SRecord(_, _, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:sleepTest"),
          dar = stableDar,
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
