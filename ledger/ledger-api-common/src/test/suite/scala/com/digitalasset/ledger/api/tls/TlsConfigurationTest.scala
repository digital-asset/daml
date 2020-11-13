// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import org.scalatest.WordSpec
import java.security.Security

import org.scalatest.Matchers

class TlsConfigurationTest extends WordSpec with Matchers {
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

  private val sslCheckRevocationProperty: String = "com.sun.net.ssl.checkRevocation"
  private val ocspProperty: String = "ocsp.enable"
  private val ENABLED = "true"
  private val DISABLED = "false"

  private def disableChecks(): Unit = {
    System.setProperty(sslCheckRevocationProperty, DISABLED)
    Security.setProperty(ocspProperty, DISABLED)
  }

  private def verifyOCSP(expectedValue: String) = {
    System.getProperty(sslCheckRevocationProperty) shouldBe expectedValue
    Security.getProperty(ocspProperty) shouldBe expectedValue
  }

}
