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

class TlsRevokedCertificatesIT
  extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxNextFixture
    with OCSPResponderFixture
{

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val clientCrt = resource("client-revoked.crt")
  val clientKey = resource("client-revoked.pem")
  val caCrt = resource("ca.crt")
  val ocspCrt = resource("ocsp.crt")
  val ocspKey = resource("ocsp.key.pem")
  val index = resource("index.txt")

  val indexPath = index.getAbsolutePath
  val caCertPath = caCrt.getAbsolutePath
  val ocspKeyPath = ocspKey.getAbsolutePath
  val ocspCertPath = ocspCrt.getAbsolutePath
  val ocspTestCertificate = clientCrt.getAbsolutePath

  private def resource(src: String) = new File(rlocation("ledger/test-common/test-certificates/" + src))

  override def clientSslContext: Option[SslContext] = clientTlsConfig.client

  protected val ledgerId =
    domain.LedgerId(s"${classOf[TlsRevokedCertificatesIT].getSimpleName.toLowerCase}-ledger-id")

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

  private def connect(): Future[LedgerClient] = {
    val config = LedgerClientConfiguration(
      applicationId = classOf[TlsRevokedCertificatesIT].getSimpleName,
      ledgerIdRequirement = LedgerIdRequirement.none,
      commandClient = CommandClientConfiguration.default,
      sslContext = clientSslContext,
      token = None,
    )
    LedgerClient(channel, config)
  }

  private lazy val serverTlsConfig = TlsConfiguration(
    enabled = true,
    keyCertChainFile = Some(serverCrt),
    keyFile = Some(serverKey),
    trustCertCollectionFile = Some(caCrt),
    revocationChecks = true
  )

  private lazy val clientTlsConfig = TlsConfiguration(
    enabled = true,
    keyCertChainFile = Some(clientCrt),
    keyFile = Some(clientKey),
    trustCertCollectionFile = Some(caCrt)
  )

}
