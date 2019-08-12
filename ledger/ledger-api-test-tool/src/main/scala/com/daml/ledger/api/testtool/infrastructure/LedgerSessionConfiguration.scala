// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.ledger.api.tls.TlsConfiguration

private[testtool] final case class LedgerSessionConfiguration(
    host: String,
    port: Int,
    ssl: Option[TlsConfiguration],
    commandTtlFactor: Double)
