// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.speedy.SValue.SRecord

final class FuncStaticTimeIT extends AbstractFuncIT {
  import AbstractScriptTest._

  protected override lazy val timeMode = ScriptTimeMode.Static

  "testSetTime" should {
    "change time and reflect the change in getTime" in {
      for {
        clients <- scriptClients()
        SRecord(_, _, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:testSetTime"),
          dar = dar,
        )
      } yield {
        assert(vals.size == 2)
        val t0 = assertSTimestamp(vals.get(0))
        val t1 = assertSTimestamp(vals.get(1))
        assert(t0 == Timestamp.assertFromString("1970-01-01T00:00:00Z"))
        assert(t1 == Timestamp.assertFromString("2000-02-02T00:01:02Z"))
      }
    }
  }
}
