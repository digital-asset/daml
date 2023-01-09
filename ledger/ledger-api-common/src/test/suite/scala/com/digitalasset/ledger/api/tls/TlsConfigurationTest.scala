// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.ledger.api.tls.TlsVersion.TlsVersion
import io.netty.handler.ssl.{OpenSslServerContext, SslContext}
import org.apache.commons.io.IOUtils
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.io.{File, InputStream}
import java.net.ConnectException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.Security

class TlsConfigurationTest extends AnyWordSpec with Matchers with BeforeAndAfterEach {

  private var systemProperties: Map[String, Option[String]] = Map.empty
  private var ocspSecurityProperty: Option[String] = None

  private val Enabled = "true"
  private val Disabled = "false"

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

  "TlsConfiguration" should {

    "configure server with TLS protocol versions" which {
      "is 1.3" in {
        getServerEnabledProtocols(Some(TlsVersion.V1_3)) shouldBe Seq("SSLv2Hello", "TLSv1.3")
      }
      "is 1.2" in {
        getServerEnabledProtocols(Some(TlsVersion.V1_2)) shouldBe Seq(
          "SSLv2Hello",
          "TLSv1.2",
          "TLSv1.3",
        )
      }
      "is default" in {
        getServerEnabledProtocols(None) shouldBe Seq(
          "SSLv2Hello",
          "TLSv1.2",
          "TLSv1.3",
        )
      }
    }

    "set OCSP JVM properties" in {
      disableChecks()
      verifyOcsp(Disabled)

      TlsConfiguration.Empty
        .copy(
          enabled = true,
          enableCertRevocationChecking = true,
        )
        .setJvmTlsProperties()

      verifyOcsp(Enabled)
    }

    "get an input stream from a plaintext private key" in {
      // given
      val keyFilePath = Files.createTempFile("private-key", ".txt")
      Files.write(keyFilePath, "private-key-123".getBytes())
      assume(Files.readAllBytes(keyFilePath) sameElements "private-key-123".getBytes)
      val keyFile = keyFilePath.toFile
      val tested = TlsConfiguration.Empty

      // when
      val actual: InputStream = tested.prepareKeyInputStream(keyFile)

      // then
      IOUtils.toString(actual, StandardCharsets.UTF_8) shouldBe "private-key-123"
    }

    "fail on missing secretsUrl when private key is encrypted ('.enc' file extension)" in {
      // given
      val keyFilePath = Files.createTempFile("private-key", ".enc")
      Files.write(keyFilePath, "private-key-123".getBytes())
      assume(Files.readAllBytes(keyFilePath) sameElements "private-key-123".getBytes)
      val keyFile = keyFilePath.toFile
      val tested = TlsConfiguration.Empty

      // when
      val e = intercept[PrivateKeyDecryptionException] {
        val _: InputStream = tested.prepareKeyInputStream(keyFile)
      }

      // then
      e.getCause shouldBe a[IllegalStateException]
      e.getCause.getMessage should endWith("cannot decrypt keyFile without secretsUrl.")
    }

    "attempt to decrypt private key using by fetching decryption params from an url" in {
      // given
      val keyFilePath = Files.createTempFile("private-key", ".enc")
      Files.write(keyFilePath, "private-key-123".getBytes())
      assume(Files.readAllBytes(keyFilePath) sameElements "private-key-123".getBytes)
      val keyFile = keyFilePath.toFile
      val tested = TlsConfiguration.Empty.copy(
        secretsUrl = Some(() => throw new ConnectException("Mocked url 123"))
      )

      // when
      val e = intercept[PrivateKeyDecryptionException] {
        val _: InputStream = tested.prepareKeyInputStream(keyFile)
      }

      // then We are not interested in decryption details (as that part is tested elsewhere).
      // We only want to verify that the decryption code path was hit (as opposed to the no-decryption code path when private key is in plaintext)
      e.getCause shouldBe a[ConnectException]
      e.getCause.getMessage shouldBe "Mocked url 123"
    }
  }

  private def configWithProtocols(minTls: Option[TlsVersion]): Option[TlsConfiguration] = {
    List("server.crt", "server.pem", "ca.crt").map { src =>
      new File(rlocation("ledger/test-common/test-certificates/" + src))
    } match {
      case List(
            certChainFilePath,
            privateKeyFilePath,
            trustCertCollectionFilePath,
          ) =>
        Some(
          TlsConfiguration(
            enabled = true,
            certChainFile = Some(certChainFilePath),
            privateKeyFile = Some(privateKeyFilePath),
            trustCollectionFile = Some(trustCertCollectionFilePath),
            minimumServerProtocolVersion = minTls,
          )
        )
      case _ => None
    }

  }

  private def getServerEnabledProtocols(minTls: Option[TlsVersion]): Seq[String] = {
    val sslContext: Option[SslContext] = configWithProtocols(minTls).flatMap(_.server)
    assume(sslContext.isDefined)
    assume(sslContext.get.isInstanceOf[OpenSslServerContext])
    TlsInfo.fromSslContext(sslContext.get).enabledProtocols
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
