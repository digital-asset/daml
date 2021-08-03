// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class WebsocketServiceIntegrationTest extends AbstractWebsocketServiceIntegrationTest {
  override protected def testId: String = getClass.getSimpleName
  websocketServiceIntegrationTests(jdbcConfig = None)
}

final class WebsocketServiceWithPostgresIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServicePostgresInt {
  override protected def testId: String = getClass.getSimpleName
  "Without table prefix" - websocketServiceIntegrationTests(this.jdbcConfig)
  "With table prefix" - websocketServiceIntegrationTests(
    Some(this.jdbcConfig_.copy(tablePrefix = "some_fancy_prefix_"))
  )
}
