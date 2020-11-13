// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script.test

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.lf.data.Ref._
import com.daml.lf.engine.script.ScriptTimeMode
import com.daml.lf.speedy.SValue
import org.scalatest._

import scala.concurrent.Future

final class TlsIT
    extends AsyncWordSpec
    with SandboxParticipantFixture
    with Matchers
    with SuiteResourceManagementAroundAll
    with OCSPResponderFixture {

  val (dar, envIface) = readDar(stableDarFile)

  val List(
    serverCrt,
    serverPem,
    caCrt,
    clientCrt,
    clientPem,
    index,
    ocspKey,
    ocspCert,
    clientRevokedCrt,
    clientRevokedPem) = {
    List(
      "server.crt",
      "server.pem",
      "ca.crt",
      "client.crt",
      "client.pem",
      "index.txt",
      "ocsp.key.pem",
      "ocsp.crt",
      "client-revoked.crt",
      "client-revoked.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  val indexPath = index.get.getAbsolutePath
  val caCertPath = caCrt.get.getAbsolutePath
  val ocspKeyPath = ocspKey.get.getAbsolutePath
  val ocspCertPath = ocspCert.get.getAbsolutePath
  val clientCertPath = clientCrt.get.getAbsolutePath

  override def timeMode = ScriptTimeMode.WallClock

  override protected def config =
    super.config
      .copy(
        tlsConfig = Some(
          TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt, revocationChecks = true)))

  "DAML Script against ledger with TLS" can {
    "test0" should {
      "ocsp property test" in {
        sys.props.get("com.sun.net.ssl.checkRevocation") shouldBe Some("true")
      }

      "create and accept Proposal" in {
        executeSampleRequest(clientCrt, clientPem)
          .map(_ => succeed) // No assertion, we just want to see that it succeeds
      }

      "fail to create and accept Proposal with a revoked client certificate" in {
        executeSampleRequest(clientRevokedCrt, clientRevokedPem).failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }
  }

  private def executeSampleRequest(
      keyCertChainFile: Option[File],
      keyFile: Option[File]): Future[SValue] = {
    participantClients(
      tlsConfiguration = TlsConfiguration(
        enabled = true,
        keyCertChainFile = keyCertChainFile,
        keyFile = keyFile,
        trustCertCollectionFile = caCrt
      )).flatMap { clients =>
      run(
        clients,
        QualifiedName.assertFromString("ScriptTest:test0"),
        dar = dar,
      )
    }
  }

}
