// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script
package test

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File

final class TlsIT
    extends AsyncWordSpec
    with CantonFixture
    with Matchers
    with SuiteResourceManagementAroundAll {

  import AbstractScriptTest._

  override protected lazy val authSecret = None
  override protected lazy val darFiles: List[File] = List(stableDarPath)
  override protected lazy val devMode: Boolean = false
  override protected lazy val nParticipants: Int = 1
  override protected lazy val timeMode = ScriptTimeMode.WallClock
  override protected lazy val tlsEnable: Boolean = true

  "Daml Script against ledger with TLS" can {
    "test0" should {
      "create and accept Proposal" in {
        for {
          clients <- scriptClients()
          _ <- run(
            clients,
            Ref.QualifiedName.assertFromString("ScriptTest:test0"),
            dar = stableDar,
          )
        } yield
        // No assertion, we just want to see that it succeeds
        succeed
      }
    }
  }
}
