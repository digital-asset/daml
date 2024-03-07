// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

final class HttpServiceWithOracleIntTestNoPayloadIndexCustomToken
    extends HttpServiceWithOracleIntTest(disableContractPayloadIndexing = true)
    with AbstractHttpServiceIntegrationTestFunsCustomToken