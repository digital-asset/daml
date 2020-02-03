// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.digitalasset.ledger.api.tls.TlsConfiguration

private[testtool] final case class LedgerSessionConfiguration(
    participants: Vector[(String, Int)],
    ssl: Option[TlsConfiguration],
    commandTtlFactor: Double,
    loadScaleFactor: Double,
)
