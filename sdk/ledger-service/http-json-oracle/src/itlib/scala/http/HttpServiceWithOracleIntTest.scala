// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class HttpServiceWithOracleIntTest(override val disableContractPayloadIndexing: Boolean)
    extends WithQueryStoreSetTest
    with HttpServiceOracleInt {

  override final def constrainedJsonQueries = !disableContractPayloadIndexing

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None
}

final class HttpServiceWithOracleIntTestCustomToken
    extends HttpServiceWithOracleIntTest(disableContractPayloadIndexing = false)
    with AbstractHttpServiceIntegrationTestFunsCustomToken

final class HttpServiceWithOracleIntTestNoPayloadIndexCustomToken
    extends HttpServiceWithOracleIntTest(disableContractPayloadIndexing = true)
    with AbstractHttpServiceIntegrationTestFunsCustomToken
