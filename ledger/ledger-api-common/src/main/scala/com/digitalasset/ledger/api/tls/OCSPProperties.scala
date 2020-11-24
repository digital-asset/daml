// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

/**
  * Enables certificate revocation checks with OCSP.
  * See: https://tersesystems.com/blog/2014/03/22/fixing-certificate-revocation/
  * See: https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.component.80.doc/security-component/jsse2Docs/knowndiffsun.html
  */
object OCSPProperties {

  val CHECK_REVOCATION_PROPERTY_SUN: String = "com.sun.net.ssl.checkRevocation"
  val CHECK_REVOCATION_PROPERTY_IBM: String = "com.ibm.jsse2.checkRevocation"
  val ENABLE_OCSP_PROPERTY: String = "ocsp.enable"

  def enableOCSP(): Unit = {
    System.setProperty(CHECK_REVOCATION_PROPERTY_SUN, TRUE)
    System.setProperty(CHECK_REVOCATION_PROPERTY_IBM, TRUE)
    java.security.Security.setProperty(ENABLE_OCSP_PROPERTY, TRUE)
  }

  private val TRUE: String = "true"

}
