// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

abstract class HttpServiceWithOracleIntTest
    extends AbstractHttpServiceIntegrationTestTokenIndependent
    with HttpServiceOracleInt {

  // XXX SC in reality, the only tests that need this to be true are "1kb of
  // data" &c.  That would entail splitting up
  // AbstractHttpServiceIntegrationTest a little; it's also possible we want to
  // run _all tests in both modes_.
  override def disableContractPayloadIndexing = true

  override def staticContentConfig: Option[StaticContentConfig] = None

  override def wsConfig: Option[WebsocketConfig] = None
}

final class HttpServiceWithOracleIntTestCustomToken
    extends HttpServiceWithOracleIntTest
    with AbstractHttpServiceIntegrationTestFunsCustomToken
