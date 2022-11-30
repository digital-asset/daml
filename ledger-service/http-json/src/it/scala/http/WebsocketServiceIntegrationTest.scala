// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class WebsocketServiceIntegrationTest extends AbstractWebsocketServiceIntegrationTest {
  override def jdbcConfig = None
}

final class WebsocketServiceIntegrationTestCustomToken
    extends WebsocketServiceIntegrationTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken

/* TODO SC uncomment
abstract class WebsocketServiceWithPostgresIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServicePostgresInt

final class WebsocketServiceWithPostgresIntTestCustomToken
    extends WebsocketServiceWithPostgresIntTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
 */
