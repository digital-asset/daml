// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.configuration

import io.netty.handler.ssl.SslContext

final case class LedgerClientConfiguration(
    applicationId: String,
    ledgerIdRequirement: LedgerIdRequirement,
    commandClient: CommandClientConfiguration,
    sslContext: Option[SslContext])
