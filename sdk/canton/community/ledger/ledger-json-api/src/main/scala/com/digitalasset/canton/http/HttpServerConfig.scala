// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import java.nio.file.Path

final case class HttpServerConfig(
    address: String = HttpServerConfig.defaultAddress,
    port: Option[Int] = None,
    portFile: Option[Path] = None,
    pathPrefix: Option[String] = None,
)

object HttpServerConfig {
  val defaultAddress: String = java.net.InetAddress.getLoopbackAddress.getHostAddress
}
