// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.daml.tls.TlsVersion.TlsVersion
import com.daml.tls.{OcspProperties, TlsInfo, TlsVersion}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.ExistingFile
import com.digitalasset.canton.networking.grpc.CantonServerBuilder
import com.digitalasset.canton.util.JarResourceUtils
import io.grpc.netty.shaded.io.netty.handler.ssl.{OpenSslServerContext, SslContext}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpec

import java.security.Security

class ServerConfigTest extends AnyWordSpec with BeforeAndAfterEach with BaseTest {

  private var systemProperties: Map[String, Option[String]] = Map.empty
  private var ocspSecurityProperty: Option[String] = None

  private val Enabled = "true"
  private val Disabled = "false"

  private val unusedClientCert: PemString = PemString(
    "-----BEGIN CERTIFICATE-----\nunused\n-----END CERTIFICATE-----"
  )

  override def beforeEach(): Unit = {
    super.beforeEach()

    systemProperties = List(
      OcspProperties.CheckRevocationPropertySun,
      OcspProperties.CheckRevocationPropertyIbm,
    ).map(name => name -> Option(System.getProperty(name))).toMap

    ocspSecurityProperty = Option(Security.getProperty(OcspProperties.EnableOcspProperty))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    systemProperties.map { case (name, value) =>
      value match {
        case Some(v) => System.setProperty(name, v)
        case None => System.clearProperty(name)
      }
    }

    Security.setProperty(OcspProperties.EnableOcspProperty, ocspSecurityProperty.getOrElse("false"))
  }

  "TlsServerConfig" should {

    "configure server with TLS protocol versions" which {
      "operates on OPENSSL" in {
        // Assert that the sbt has managed to pull in openssl. The tests are meaningless if it hasn't.
        // If that happens, the root cause needs to be investigated as very likely it will also affect
        // canton at runtime.
        val sslContext: Option[SslContext] =
          configWithProtocols(Some(TlsVersion.V1_3)).map(CantonServerBuilder.sslContext(_))
        sslContext should not be empty
        sslContext.foreach(_ shouldBe a[OpenSslServerContext])
      }
      "with 1.3" in {
        getServerEnabledProtocols(Some(TlsVersion.V1_3)) should contain theSameElementsAs Seq(
          "SSLv2Hello",
          "TLSv1.3",
        )
      }
      "with 1.2" in {
        getServerEnabledProtocols(Some(TlsVersion.V1_2)) should contain theSameElementsAs Seq(
          "SSLv2Hello",
          "TLSv1.2",
          "TLSv1.3",
        )
      }
      "with default" in {
        getServerEnabledProtocols(None) should contain theSameElementsAs Seq(
          "SSLv2Hello",
          "TLSv1.2",
          "TLSv1.3",
        )
      }
    }

    "set OCSP JVM properties" in {
      disableChecks()
      verifyOcsp(Disabled)

      TlsServerConfig(
        certChainFile =
          PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile("tls/public-api.crt"))),
        privateKeyFile =
          PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile("tls/public-api.pem"))),
        enableCertRevocationChecking = true,
      ).setJvmTlsProperties()

      verifyOcsp(Enabled)
    }

  }

  private def configWithProtocols(minTls: Option[TlsVersion]): Option[TlsServerConfig] =
    List("ledger-api.crt", "ledger-api.pem", "root-ca.crt", "some.pem").map { src =>
      PemFile(ExistingFile.tryCreate(JarResourceUtils.resourceFile("tls/" + src)))
    } match {
      case List(
            certChainFile,
            privateKeyFile,
            trustCollectionFile,
            unusedClientKey,
          ) =>
        Some(
          TlsServerConfig(
            certChainFile = certChainFile,
            privateKeyFile = privateKeyFile,
            trustCollectionFile = Some(trustCollectionFile),
            clientAuth = ServerAuthRequirementConfig.Require(
              TlsClientCertificate(unusedClientCert, unusedClientKey)
            ),
            minimumServerProtocolVersion = minTls.map(_.version),
          )
        )
      case _ => None
    }

  private def getServerEnabledProtocols(minTls: Option[TlsVersion]): Seq[String] = {
    val sslContext: Option[SslContext] =
      configWithProtocols(minTls).map(CantonServerBuilder.sslContext(_))
    assume(sslContext.isDefined)
    inside(sslContext) { case Some(theContext) =>
      assume(theContext.isInstanceOf[OpenSslServerContext])
      TlsInfo.fromSslContext(theContext).enabledProtocols
    }
  }

  private def disableChecks(): Unit = {
    System.setProperty(OcspProperties.CheckRevocationPropertySun, Disabled)
    System.setProperty(OcspProperties.CheckRevocationPropertyIbm, Disabled)
    Security.setProperty(OcspProperties.EnableOcspProperty, Disabled)
  }

  private def verifyOcsp(expectedValue: String) = {
    System.getProperty(OcspProperties.CheckRevocationPropertySun) shouldBe expectedValue
    System.getProperty(OcspProperties.CheckRevocationPropertyIbm) shouldBe expectedValue
    Security.getProperty(OcspProperties.EnableOcspProperty) shouldBe expectedValue
  }

}
