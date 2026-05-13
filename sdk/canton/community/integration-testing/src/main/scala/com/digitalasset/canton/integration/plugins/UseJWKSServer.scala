// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.daml.http.test.SimpleHttpServer
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.EnvironmentSetupPlugin
import com.sun.net.httpserver.HttpServer

class UseJWKSServer(jwks: String, altJwks: String) extends EnvironmentSetupPlugin with BaseTest {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  private[integration] var server: HttpServer = _
  private[integration] var altServer: HttpServer = _

  override def beforeTests(): Unit = {
    server = SimpleHttpServer.start(jwks)
    altServer = SimpleHttpServer.start(altJwks)
  }

  lazy val endpoint: String = SimpleHttpServer.responseUrl(server)
  lazy val altEndpoint: String = SimpleHttpServer.responseUrl(altServer)

  override def afterTests(): Unit = {
    SimpleHttpServer.stop(server)
    SimpleHttpServer.stop(altServer)
  }
}
