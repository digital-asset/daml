// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.config.RequireTypes.Port

import java.nio.file.Path
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** JSON API HTTP server configuration.
  *
  * @param address
  *   The address to bind the HTTP server to. Defaults to the loopback address.
  * @param port
  *   The port to bind the HTTP server to. If not specified, the port will be dynamically assigned.
  * @param portFile
  *   Optional file to write the port number to after the server starts. This is useful for other
  *   processes to discover the port on which the server is running.
  * @param pathPrefix
  *   The path prefix for the HTTP server. If specified, all routes will be prefixed with this path.
  * @param requestTimeout
  *   The timeout for HTTP requests. Defaults to 20 seconds. Increase this value if you expect to
  *   handle long-running requests. As the backing server is implemented by Pekko HTTP, this
  *   configuration overrides the provided Pekko configuration (pekko.http.server.request-timeout).
  */
final case class HttpServerConfig(
    address: String = HttpServerConfig.defaultAddress,
    internalPort: Option[Port] = None,
    portFile: Option[Path] = None,
    pathPrefix: Option[String] = None,
    requestTimeout: FiniteDuration = HttpServerConfig.defaultRequestTimeout,
) {
  def port: Port =
    internalPort.getOrElse(
      throw new IllegalStateException("Accessing server port before default was set")
    )
}

object HttpServerConfig {
  private val defaultAddress: String = java.net.InetAddress.getLoopbackAddress.getHostAddress
  private val defaultRequestTimeout: FiniteDuration = 20.seconds
}
