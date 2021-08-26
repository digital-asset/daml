// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class WebsocketServiceWithOracleIntTest
    extends AbstractWebsocketServiceIntegrationTest
    with HttpServiceOracleInt {
  override def disableContractPayloadIndexing = false
}
