// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class WebsocketServiceWithOracleIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServiceOracleInt {
  override def disableContractPayloadIndexing = false
}

final class WebsocketServiceWithOracleIntTestCustomToken
    extends WebsocketServiceWithOracleIntTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
