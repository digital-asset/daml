// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.daml.http.test.SimpleHttpServer
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.sun.net.httpserver.HttpServer

class UseJWKSServer(jwks: String) extends EnvironmentSetupPlugin with BaseTest {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private[integration] var server: HttpServer = _

  override def beforeTests(): Unit =
    server = SimpleHttpServer.start(jwks)

  lazy val endpoint: String = SimpleHttpServer.responseUrl(server)

  override def afterTests(): Unit = SimpleHttpServer.stop(server)
}
