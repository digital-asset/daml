package com.daml.platform.sandboxnext

import java.io.File

import org.scalatest.AsyncWordSpec
import org.scalatest.Matchers

class TlsRevokedCertificatesIT
  extends AsyncWordSpec
    with Matchers
    with SandboxWithOCSPFixture {

  override val clientCrt: File = resource("client-revoked.crt")
  override val clientKey: File = resource("client-revoked.pem")

  "The test runner" should {
    "be configured to enable OCSP revocation checks" in {
      sys.props.get("com.sun.net.ssl.checkRevocation") shouldBe Some("true")
      java.security.Security.getProperty("ocsp.enable") shouldBe "true"
    }
  }

  "The Ledger API Server" should {
    "not allow connections with revoked TLS certificates" in {
      connect()
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
