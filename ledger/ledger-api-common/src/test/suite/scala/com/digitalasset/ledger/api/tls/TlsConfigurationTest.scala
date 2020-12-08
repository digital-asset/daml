// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.security.Security

import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TlsConfigurationTest extends AnyWordSpec with Matchers with BeforeAndAfterEach {

  var systemProperties: Map[String, Option[String]] = Map.empty
  var ocspSecurityProperty: Option[String] = None

  override def beforeEach(): Unit = {
    super.beforeEach()

    systemProperties = List(
      OcspProperties.CheckRevocationPropertySun,
      OcspProperties.CheckRevocationPropertyIbm
    ).map(name => name -> Option(System.getProperty(name))).toMap

    ocspSecurityProperty = Option(Security.getProperty(OcspProperties.EnableOcspProperty))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    systemProperties.map {
      case (name, value) =>
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
          enableCertRevocationChecking = true
        )
        .setJvmTlsProperties()

      verifyOcsp(Enabled)
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
