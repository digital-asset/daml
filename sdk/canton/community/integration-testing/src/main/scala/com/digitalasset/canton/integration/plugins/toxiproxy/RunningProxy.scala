// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins.toxiproxy

import com.digitalasset.canton.BaseTest
import eu.rekawek.toxiproxy
import eu.rekawek.toxiproxy.ToxiproxyClient

/** A [[toxiproxy.Proxy]] that is currently active.
  * @param underlyingProxy
  *   The proxy instance.
  * @param container
  *   The container that the proxy is running in. Only set when running tests locally.
  * @param controller
  *   A toxiproxy client that is configured to control the proxy, e.g. to add toxics.
  */
class RunningProxy(
    underlyingProxy: toxiproxy.Proxy,
    container: Option[UseToxiproxy.ToxiproxyContainer],
    controller: ToxiproxyClient,
) extends BaseTest {
  val underlying: toxiproxy.Proxy = underlyingProxy
  val controllingToxiproxyClient: ToxiproxyClient = controller

  def portFromHost: Int = container match {
    case Some(value) =>
      val port: Int = portFromToxiproxyHost
      value.getMappedPort(port).intValue()
    case None =>
      portFromToxiproxyHost
  }

  def ipFromHost: String = container match {
    case Some(value) =>
      value.getHost
    case None => {
      "localhost"
    }
  }

  private def portFromToxiproxyHost = {
    val listen = underlyingProxy.getListen
    val port = listen.split(":").last.toInt
    port
  }

}
