// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.tls

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.resources.TestResourceContext
import com.daml.platform.apiserver.LedgerApiServer
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class TlsCertificateRevocationCheckingSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll
    with TestResourceContext
    with OcspResponderFixture {
  import TlsCertificateRevocationCheckingSpec.resource

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val clientCrt = resource("client.crt")
  val clientKey = resource("client.pem")
  val clientRevokedCrt = resource("client-revoked.crt")
  val clientRevokedKey = resource("client-revoked.pem")
  val ocspCrt = resource("ocsp.crt")
  val ocspKey = resource("ocsp.key.pem")
  val index = resource("index.txt")

  override protected def indexPath: String = index.getAbsolutePath
  override protected def caCertPath: String = caCrt.getAbsolutePath
  override protected def ocspKeyPath: String = ocspKey.getAbsolutePath
  override protected def ocspCertPath: String = ocspCrt.getAbsolutePath
  override protected def ocspTestCertificate: String = clientCrt.getAbsolutePath

  classOf[LedgerApiServer].getSimpleName when {
    "certificate revocation checking is enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientCrt),
          Some(clientKey),
          certRevocationChecking = true,
        )
          .makeARequest()
          .map(_ => succeed)
      }

      "block TLS connections with revoked certificates" in {
        TlsFixture(
          tlsEnabled = true,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientRevokedCrt),
          Some(clientRevokedKey),
          certRevocationChecking = true,
        )
          .makeARequest()
          .failed
          .collect {
            case com.daml.grpc.GrpcException.UNAVAILABLE() =>
              succeed
            case ex =>
              fail(s"Invalid exception: ${ex.getClass.getCanonicalName}: ${ex.getMessage}")
          }
      }
    }

    "certificate revocation checking is not enabled" should {
      "allow TLS connections with valid certificates" in {
        TlsFixture(
          tlsEnabled = false,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientCrt),
          Some(clientKey),
        )
          .makeARequest()
          .map(_ => succeed)
      }

      "allow TLS connections with revoked certificates" in {
        TlsFixture(
          tlsEnabled = false,
          serverCrt,
          serverKey,
          caCrt,
          Some(clientRevokedCrt),
          Some(clientRevokedKey),
        )
          .makeARequest()
          .map(_ => succeed)
      }
    }
  }
}

object TlsCertificateRevocationCheckingSpec {

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))

}
