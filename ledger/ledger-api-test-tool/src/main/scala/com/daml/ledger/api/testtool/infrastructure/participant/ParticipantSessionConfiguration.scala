// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.digitalasset.ledger.api.tls.TlsConfiguration

private[testtool] final case class ParticipantSessionConfiguration(
    host: String,
    port: Int,
    ssl: Option[TlsConfiguration],
    commandTtlFactor: Double,
)
