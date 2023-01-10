// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class WebsocketServiceIntegrationTest
    extends AbstractWebsocketServiceIntegrationTest(
      integration = "w/o DB"
    ) {
  override def jdbcConfig = None
}

final class WebsocketServiceIntegrationTestCustomToken
    extends WebsocketServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken

abstract class WebsocketServiceWithPostgresIntTest
    extends AbstractWebsocketServiceIntegrationTest("w/ Postgres")
    with HttpServicePostgresInt

final class WebsocketServiceWithPostgresIntTestCustomToken
    extends WebsocketServiceWithPostgresIntTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
