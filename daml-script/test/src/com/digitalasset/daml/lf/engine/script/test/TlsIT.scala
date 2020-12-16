// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

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

  override protected def config =
    super.config
      .copy(tlsConfig = Some(TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)))

  "DAML Script against ledger with TLS" can {
    "test0" should {
      "create and accept Proposal" in {
        for {
          clients <- participantClients(
            tlsConfiguration = TlsConfiguration(
              enabled = true,
              keyCertChainFile = clientCrt,
              keyFile = clientPem,
              trustCertCollectionFile = caCrt
            ))
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
