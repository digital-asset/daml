package com.daml.platform.sandboxnext

import java.io.File

import org.scalatest.AsyncWordSpec
import org.scalatest.Matchers

class TlsIT
  extends AsyncWordSpec
    with Matchers
    with SandboxWithOCSPFixture {

  override val clientCrt: File = resource("client.crt")
  override val clientKey: File = resource("client.pem")

  "The test runner" should {
    "be configured to enable OCSP revocation checks" in {
      sys.props.get("com.sun.net.ssl.checkRevocation") shouldBe Some("true")
      java.security.Security.getProperty("ocsp.enable") shouldBe "true"
    }
  }

  "The Ledger API Server" should {
    "allow connections with valid TLS certificates" in {
      connect().map { client =>
        client.ledgerId should be(ledgerId)
      }
    }
  }

}
