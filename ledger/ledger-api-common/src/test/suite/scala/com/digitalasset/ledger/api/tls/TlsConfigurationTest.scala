// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import org.scalatest.WordSpec
import java.security.Security

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers

class TlsConfigurationTest extends WordSpec with Matchers with BeforeAndAfterEach {

  var systemProperties: Map[String, Option[String]] = Map.empty
  var ocspSecurityProperty: Option[String] = None

  override def beforeEach(): Unit = {
    super.beforeEach()

    systemProperties = List(
      OCSPProperties.CHECK_REVOCATION_PROPERTY_SUN,
      OCSPProperties.CHECK_REVOCATION_PROPERTY_IBM
    ).map(name => name -> Option(System.getProperty(name))).toMap

    ocspSecurityProperty = Option(Security.getProperty(OCSPProperties.ENABLE_OCSP_PROPERTY))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    systemProperties.map { case (name, value) =>
      value match {
        case Some(v) => System.setProperty(name, v)
        case None => System.clearProperty(name)
      }
    }

    Security.setProperty(OCSPProperties.ENABLE_OCSP_PROPERTY, ocspSecurityProperty.getOrElse("false"))
  }

  "TlsConfiguration" should {
    "set OCSP JVM properties" in {
      disableChecks()
      verifyOCSP(DISABLED)

      TlsConfiguration.Empty
        .copy(
          enabled = true,
          revocationChecks = true
        )
        .setJvmTlsProperties()

      verifyOCSP(ENABLED)
    }
  }

  private def disableChecks(): Unit = {
    System.setProperty(OCSPProperties.CHECK_REVOCATION_PROPERTY_SUN, DISABLED)
    System.setProperty(OCSPProperties.CHECK_REVOCATION_PROPERTY_IBM, DISABLED)
    Security.setProperty(OCSPProperties.ENABLE_OCSP_PROPERTY, DISABLED)
  }

  private def verifyOCSP(expectedValue: String) = {
    System.getProperty(OCSPProperties.CHECK_REVOCATION_PROPERTY_SUN) shouldBe expectedValue
    System.getProperty(OCSPProperties.CHECK_REVOCATION_PROPERTY_IBM) shouldBe expectedValue
    Security.getProperty(OCSPProperties.ENABLE_OCSP_PROPERTY) shouldBe expectedValue
  }

  private val ENABLED = "true"
  private val DISABLED = "false"

}
