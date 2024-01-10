// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.lf.data.Ref
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.language.LanguageMajorVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class TlsITV2 extends TlsIT(LanguageMajorVersion.V2)

class TlsIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  final override protected lazy val tlsEnable = true
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

  // TODO(#17366): Delete once 2.0 is introduced and Canton supports LF v2 in non-dev mode.
  final override protected lazy val devMode = (majorLanguageVersion == LanguageMajorVersion.V2)

  "Daml Script against ledger with TLS" can {
    "test0" should {
      "create and accept Proposal" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("ScriptTest:test0"),
            dar = dar,
          )
        } yield
        // No assertion, we just want to see that it succeeds
        succeed
      }
    }
  }
}
