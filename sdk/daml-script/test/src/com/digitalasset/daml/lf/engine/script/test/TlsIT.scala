// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.lf.data.Ref
import com.daml.lf.engine.script.ScriptTimeMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class TlsIT extends AsyncWordSpec with AbstractScriptTest with Matchers {

  import AbstractScriptTest._

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
