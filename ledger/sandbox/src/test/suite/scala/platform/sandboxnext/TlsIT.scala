package com.daml.platform.sandboxnext

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ledger.client.configuration.LedgerIdRequirement
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import io.netty.handler.ssl.SslContext
import org.scalatest.AsyncWordSpec
import org.scalatest.Matchers

import scala.concurrent.Future

class TlsIT
  extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxNextFixture
    with OCSPResponderFixture
{

  val List(
  serverCrt,
  serverKey,
  clientCrt,
  clientKey,
  caCrt,
  index,
  ocspKey,
  ocspCert) = {
    List(
      "server.crt",
      "server.pem",
      "client.crt",
      "client.pem",
      "ca.crt",
      "index.txt",
      "ocsp.key.pem",
      "ocsp.crt").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  val indexPath = index.get.getAbsolutePath
  val caCertPath = caCrt.get.getAbsolutePath
  val ocspKeyPath = ocspKey.get.getAbsolutePath
  val ocspCertPath = ocspCert.get.getAbsolutePath
  val ocspTestCertificate = clientCrt.get.getAbsolutePath

  override def clientSslContext: Option[SslContext] = clientTlsConfig.client

  protected val ledgerId =
    domain.LedgerId(s"${classOf[TlsIT].getSimpleName.toLowerCase}-ledger-id")

  override protected def config: SandboxConfig = super.config.copy(
    ledgerIdMode = LedgerIdMode.Static(ledgerId),
    tlsConfig = Some(serverTlsConfig)
  )

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

  private def connect(): Future[LedgerClient] = {
    val config = LedgerClientConfiguration(
      applicationId = classOf[TlsIT].getSimpleName,
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = clientSslContext,
      token = None,
    )
    LedgerClient(channel, config)
  }

  private lazy val serverTlsConfig = TlsConfiguration(
    enabled = true,
    keyCertChainFile = serverCrt,
    keyFile = serverKey,
    trustCertCollectionFile = caCrt,
    revocationChecks = true
  )

  private lazy val clientTlsConfig = TlsConfiguration(
    enabled = true,
    keyCertChainFile = clientCrt,
    keyFile = clientKey,
    trustCertCollectionFile = caCrt
  )

}
