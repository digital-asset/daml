// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class WebsocketServiceIntegrationTest extends AbstractWebsocketServiceIntegrationTest {
  override def jdbcConfig = None
}

final class WebsocketServiceWithPostgresIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServicePostgresInt
