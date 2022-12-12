// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.sandbox.SandboxOnXForTest.{ApiServerConfig, singleParticipant}
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ledgerinteraction.ScriptTimeMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File

final class TlsIT
    extends AsyncWordSpec
    with SandboxParticipantFixture
    with Matchers
    with SuiteResourceManagementAroundAll {
  val (dar, envIface) = readDar(stableDarFile)

  val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  override def timeMode = ScriptTimeMode.WallClock

  override def config = super.config.copy(
    participants = singleParticipant(
      ApiServerConfig.copy(
        tls = Some(TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt))
      )
    )
  )

  "Daml Script against ledger with TLS" can {
    "test0" should {
      "create and accept Proposal" in {
        for {
          clients <- participantClients(
            tlsConfiguration = TlsConfiguration(
              enabled = true,
              certChainFile = clientCrt,
              privateKeyFile = clientPem,
              trustCollectionFile = caCrt,
            )
          )
          _ <- run(
            clients,
            QualifiedName.assertFromString("ScriptTest:test0"),
            dar = dar,
          )
        } yield
        // No assertion, we just want to see that it succeeds
        succeed
      }
    }
  }
}
