// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.tls.TlsConfiguration
import com.digitalasset.canton.pureconfigutils.HttpServerConfig

final case class HttpApiConfig(
    server: HttpServerConfig = HttpServerConfig(),
    websocketConfig: Option[WebsocketConfig] = None,
    allowInsecureTokens: Boolean = false,
    staticContent: Option[StaticContentConfig] = None,
    debugLoggingOfHttpBodies: Boolean = false,
) {

  // TODO(#13303) Use directly instead of using JsonApiConfig as indirection
  def toConfig(tls: Option[TlsConfiguration]): JsonApiConfig = {
    JsonApiConfig(
      address = server.address,
      httpPort = server.port,
      httpsConfiguration = tls,
      portFile = server.portFile,
      staticContentConfig = staticContent,
      allowNonHttps = allowInsecureTokens,
      wsConfig = websocketConfig,
      debugLoggingOfHttpBodies = debugLoggingOfHttpBodies,
    )
  }
}
