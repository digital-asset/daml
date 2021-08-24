// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import org.apache.commons.io.IOUtils
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.InputStream
import java.net.{ConnectException, URL}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.Security

class TlsConfigurationTest extends AnyWordSpec with Matchers with BeforeAndAfterEach {

  var systemProperties: Map[String, Option[String]] = Map.empty
  var ocspSecurityProperty: Option[String] = None

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
      val e = intercept[IllegalStateException] {
        val _: InputStream = tested.prepareKeyInputStream(keyFile)
      }

      // then
      e.getMessage shouldBe "Unable to convert TlsConfiguration(true,None,None,None,None,REQUIRE,false,List()) to SSL Context: cannot decrypt keyFile without secretsUrl."
    }

    "attempt to decrypt private key using by fetching decryption params from an url" in {
      // given
      val keyFilePath = Files.createTempFile("private-key", ".enc")
      Files.write(keyFilePath, "private-key-123".getBytes())
      assume(Files.readAllBytes(keyFilePath) sameElements "private-key-123".getBytes)
      val keyFile = keyFilePath.toFile
      val url = new URL("http://localhost/this/does/not/exist/sdkjfsldf")
      val tested = TlsConfiguration.Empty
        .copy(secretsUrl = Some(url))

      // when & then: key decryption logic should attempt to connect to the url
      assertThrows[ConnectException] {
        val _: InputStream = tested.prepareKeyInputStream(keyFile)
      }
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

  private val Enabled = "true"
  private val Disabled = "false"

}
