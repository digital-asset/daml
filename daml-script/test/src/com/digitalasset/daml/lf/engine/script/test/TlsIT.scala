// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.lf.data.Ref._
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

  "Daml Script against ledger with TLS" can {
    "test0" should {
      "create and accept Proposal" in {
        for {
          clients <- participantClients()
          _ <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:test0"),
            dar = stableDar,
          )
        } yield
        // No assertion, we just want to see that it succeeds
        succeed
      }
    }
  }

  override protected val timeMode: ScriptTimeMode = ScriptTimeMode.WallClock
  override protected def darFiles: List[File] = List(stableDarPath)
  override protected def nParticipants: Int = 1
  override protected def devMode: Boolean = false
  override protected def tlsEnable: Boolean = true
}
