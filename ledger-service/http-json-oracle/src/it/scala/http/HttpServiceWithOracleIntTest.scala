// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

class HttpServiceWithOracleIntTest
    extends AbstractHttpServiceIntegrationTest
    with HttpServiceOracleInt {

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None

  "Without table prefix" - httpServiceIntegrationTests(this.jdbcConfig)
  "With table prefix" - httpServiceIntegrationTests(
    this.jdbcConfig.map(_.copy(tablePrefix = "some_fancy_prefix_"))
  )
}
