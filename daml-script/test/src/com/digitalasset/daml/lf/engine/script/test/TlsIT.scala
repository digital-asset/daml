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
    with SuiteResourceManagementAroundAll {
  // TODO: this should go to a proper place (for testing only here)
  //  System.setProperty("com.sun.net.ssl.checkRevocation", "true")
  //  System.setProperty("com.sun.security.enableCRLDP", "true")
  java.security.Security.setProperty("ocsp.enable", "true")

  val (dar, envIface) = readDar(stableDarFile)

  val List(serverCrt, serverPem, caCrt, clientCrt, clientPem, index, ocspKey, ocspCert, clientRevokedCrt, clientRevokedPem) = {
    List("server.cert.pem", "server.pem", "ca.cert.pem", "client.cert.pem", "client.pem", "index.txt", "ocsp.key.pem", "ocsp.cert.pem", "client-revoked.cert.pem", "client-revoked.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  // TODO: remove this
  val indexPath = index.get.getAbsolutePath
  val caCertPath = caCrt.get.getAbsolutePath
  val ocspKeyPath = ocspKey.get.getAbsolutePath
  val ocspCertPath = ocspCert.get.getAbsolutePath
  val clientCertPath = clientCrt.get.getAbsolutePath

  println(indexPath)
  println(caCertPath)
  println(ocspKeyPath)
  println(ocspCertPath)

  // TODO: this will go to a separate trait that extends BeforeAndAfterAll
  val command = List(
    "/usr/local/opt/openssl/bin/openssl",
    "ocsp",
    "-port",
    "2560",
    "-text",
    "-index", indexPath,
    "-CA", caCertPath,
    "-rkey", ocspKeyPath,
    "-rsigner", ocspCertPath
  )

  Future {
    scala.sys.process.Process(command).run()
  }
  Thread.sleep(10000)
  // TODO: end of BeforeAndAfterAll

  override def timeMode = ScriptTimeMode.WallClock

  override protected def config =
    super.config
      .copy(tlsConfig = Some(TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)))

  private def executeSampleRequest(keyCertChainFile: Option[File], keyFile: Option[File]): Future[SValue] = {
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

  "Test OCSP responder" can {
    // TODO: move this to the ocsp trait
    "respond to a revocation check request" in {
      for {
        _ <- Future.successful(Thread.sleep(5000))
        _ <- Future.successful("VERIFY REVOCATION STATUS")
        x <- Future {
          val cmd = List(
            "/usr/local/opt/openssl/bin/openssl",
            "ocsp",
            "-CAfile",
            caCertPath,
            "-url",
            "http://127.0.0.1:2560",
            "-resp_text",
            "-issuer",
            caCertPath,
            "-cert",
            clientCertPath
          )
          scala.sys.process.Process(cmd).!!
        }
        _ = println(s"RESPONSE: ${x.take(10)}")
      } yield succeed
    }
  }

  "DAML Script against ledger with TLS" can {
    "test0" should {
      // TODO: remove this
      "ocsp property test" in {
        sys.props.get("com.sun.net.ssl.checkRevocation") shouldBe Some("true")
      }


      "create and accept Proposal" in {
        Thread.sleep(5000)
        executeSampleRequest(clientCrt, clientPem)
          .map(_ => succeed) // No assertion, we just want to see that it succeeds
      }

      "fail to create and accept Proposal with a revoked client certificate" in {
        Thread.sleep(5000)
        executeSampleRequest(clientRevokedCrt, clientRevokedPem)
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }
  }
}
