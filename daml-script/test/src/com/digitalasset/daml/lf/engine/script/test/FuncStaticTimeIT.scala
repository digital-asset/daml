// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.lf.data.Ref.QualifiedName
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SValue.SRecord

class FuncStaticTimeITV1 extends  FuncStaticTimeIT(LanguageMajorVersion.V1)
// TODO(#17366): Uncomment once we can ask Canton to use a particular dev version. For now it
//   defaults to 1.dev.
//class FuncStaticTimeITV2 extends  FuncStaticTimeIT(LanguageMajorVersion.V2)

class FuncStaticTimeIT(override val majorLanguageVersion: LanguageMajorVersion) extends AbstractFuncIT {

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
