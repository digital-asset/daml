// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script
package test

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.engine.script.ScriptTimeMode
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class TlsITV2 extends TlsIT(LanguageMajorVersion.V2)

class TlsIT(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractScriptTest
    with Matchers {

  final override protected lazy val tlsEnable = true
  final override protected lazy val timeMode = ScriptTimeMode.WallClock

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
