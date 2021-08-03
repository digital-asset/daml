// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class WebsocketServiceWithOracleIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServiceOracleInt {
  "Without table prefix" - websocketServiceIntegrationTests(this.jdbcConfig)
  "With table prefix" - websocketServiceIntegrationTests(
    Some(this.jdbcConfig_.copy(tablePrefix = "some_fancy_prefix_"))
  )
}
