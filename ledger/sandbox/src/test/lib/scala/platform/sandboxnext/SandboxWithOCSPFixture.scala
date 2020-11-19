// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.daml.ledger.api.tls.OCSPProperties
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.CommandClientConfiguration
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.ledger.client.configuration.LedgerIdRequirement
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import io.netty.handler.ssl.SslContext
import org.scalatest.Suite

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

trait SandboxWithOCSPFixture
    extends AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxNextFixture
    with OCSPResponderFixture {
  this: Suite =>

  private implicit val ec: ExecutionContext = system.dispatcher

  val clientCrt: File
  val clientKey: File

  val serverCrt = resource("server.crt")
  val serverKey = resource("server.pem")
  val caCrt = resource("ca.crt")
  val ocspCrt = resource("ocsp.crt")
  val ocspKey = resource("ocsp.key.pem")
  val index = resource("index.txt")

  override protected def indexPath = index.getAbsolutePath
  override protected def caCertPath = caCrt.getAbsolutePath
  override protected def ocspKeyPath = ocspKey.getAbsolutePath
  override protected def ocspCertPath = ocspCrt.getAbsolutePath
  override protected def ocspTestCertificate = clientCrt.getAbsolutePath

  override def beforeEach(): Unit = {
    super.beforeEach()
    assertOCSPPropertiesConfigured()
    ()
  }

  private def assertOCSPPropertiesConfigured() = {
    assert(sys.props.get(OCSPProperties.CHECK_REVOCATION_PROPERTY_SUN).contains("true"))
    assert(sys.props.get(OCSPProperties.CHECK_REVOCATION_PROPERTY_IBM).contains("true"))
    assert(java.security.Security.getProperty(OCSPProperties.ENABLE_OCSP_PROPERTY) == "true")
  }

  override protected def clientSslContext: Option[SslContext] = clientTlsConfig.client

  private val testIdentifier = "OCSPIT"
  protected val ledgerId =
    domain.LedgerId(s"$testIdentifier-ledger-id")

  override protected def config: SandboxConfig = super.config.copy(
    ledgerIdMode = LedgerIdMode.Static(ledgerId),
    tlsConfig = Some(serverTlsConfig)
  )

  protected def connect(): Future[LedgerClient] = {
    val config = LedgerClientConfiguration(
      applicationId = testIdentifier,
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

  protected def resource(src: String) =
    new File(rlocation("ledger/test-common/test-certificates/" + src))

}
