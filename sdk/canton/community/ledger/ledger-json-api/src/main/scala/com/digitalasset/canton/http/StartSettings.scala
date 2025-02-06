// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

// defined separately from Config so
//  1. it is absolutely lexically apparent what `import startSettings._` means
//  2. avoid incorporating other Config'd things into "the shared args to start"
trait StartSettings {
  val server: HttpServerConfig
  val websocketConfig: Option[WebsocketConfig]
  val debugLoggingOfHttpBodies: Boolean
  val damlDefinitionsServiceEnabled: Boolean
  val userManagementWithoutAuthorization: Boolean
}
