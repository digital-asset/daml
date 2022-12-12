// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.security.Security

/** Disables the unwanted legacy SSLv2Hello protocol at the JSSE level.
  * See: https://www.java.com/en/configure_crypto.html#DisableTLS:~:text=Disable%20TLS%201.0%20and%20TLS%201.1
  */
object ProtocolDisabler {
  val disabledAlgorithmsProperty: String = "jdk.tls.disabledAlgorithms"
  val sslV2Protocol: String = "SSLv2Hello"

  def disableSSLv2Hello(): Unit =
    PropertiesUpdater(Security.getProperty, Security.setProperty)
      .appendToProperty(disabledAlgorithmsProperty, sslV2Protocol)
}

private[tls] case class PropertiesUpdater(
    getter: String => String,
    setter: (String, String) => Unit,
) {
  def appendToProperty(name: String, value: String): Unit = {
    val property = getter(name)
    val fullProperty =
      property
        .split(",")
        .map(_.trim)
        .find(_ == value)
        .map(_ => property)
        .getOrElse(s"$property, $value")
    setter(name, fullProperty)
  }
}
