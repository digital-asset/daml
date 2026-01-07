// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script.test

import com.digitalasset.daml.lf.data.Ref.QualifiedName
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.value.Value._

class FuncStaticTimeITV2 extends FuncStaticTimeIT(LanguageVersion.Major.V2)

class FuncStaticTimeIT(override val majorLanguageVersion: LanguageVersion.Major)
    extends AbstractFuncIT {

  protected override lazy val timeMode = ScriptTimeMode.Static

  "testSetTime" should {
    "change time and reflect the change in getTime" in {
      for {
        clients <- scriptClients()
        ValueRecord(_, vals) <- run(
          clients,
          QualifiedName.assertFromString("ScriptTest:testSetTime"),
          dar = dar,
        )
      } yield {
        assert(vals.length == 2)
        val t0 = assertValueTimestamp(vals(0)._2)
        val t1 = assertValueTimestamp(vals(1)._2)
        assert(t0 == Timestamp.assertFromString("1970-01-01T00:00:00Z"))
        assert(t1 == Timestamp.assertFromString("2000-02-02T00:01:02Z"))
      }
    }
  }
}
