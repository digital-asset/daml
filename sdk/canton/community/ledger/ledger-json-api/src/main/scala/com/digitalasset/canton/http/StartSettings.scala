// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

trait StartSettings {
  val server: HttpServerConfig
  val websocketConfig: Option[WebsocketConfig]
  val debugLoggingOfHttpBodies: Boolean
  val damlDefinitionsServiceEnabled: Boolean
  val userManagementWithoutAuthorization: Boolean
}
