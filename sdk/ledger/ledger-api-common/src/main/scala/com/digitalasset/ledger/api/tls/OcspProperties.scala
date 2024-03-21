// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

/** Enables certificate revocation checks with OCSP.
  * See: https://tersesystems.com/blog/2014/03/22/fixing-certificate-revocation/
  * See: https://www.ibm.com/support/knowledgecenter/en/SSYKE2_8.0.0/com.ibm.java.security.component.80.doc/security-component/jsse2Docs/knowndiffsun.html
  */
object OcspProperties {

  val CheckRevocationPropertySun: String = "com.sun.net.ssl.checkRevocation"
  val CheckRevocationPropertyIbm: String = "com.ibm.jsse2.checkRevocation"
  val EnableOcspProperty: String = "ocsp.enable"

  def enableOcsp(): Unit = {
    System.setProperty(CheckRevocationPropertySun, True)
    System.setProperty(CheckRevocationPropertyIbm, True)
    java.security.Security.setProperty(EnableOcspProperty, True)
  }

  private val True: String = "true"

}
