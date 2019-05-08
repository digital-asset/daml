// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.configuration
import com.digitalasset.ledger.client.grpcHeaders.AuthorizationConfig
import io.netty.handler.ssl.SslContext

final case class LedgerClientConfiguration(
    applicationId: String,
    ledgerIdRequirement: LedgerIdRequirement,
    commandClient: CommandClientConfiguration,
    sslContext: Option[SslContext],
    authorizationConfig: Option[AuthorizationConfig])

object LedgerClientConfiguration {
  def default = LedgerClientConfiguration(
    applicationId = "",
    ledgerIdRequirement = LedgerIdRequirement.default,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    authorizationConfig = None
  )
}