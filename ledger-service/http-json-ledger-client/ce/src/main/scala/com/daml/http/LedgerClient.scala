// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import com.daml.ledger.client.configuration.LedgerClientChannelConfiguration
import io.grpc.netty.NettyChannelBuilder

import scala.concurrent.{ExecutionContext, Future}

object LedgerClient extends LedgerClientBase {

  def channelBuilder(
      ledgerHost: String,
      ledgerPort: Int,
      clientChannelConfig: LedgerClientChannelConfiguration,
  )(implicit executionContext: ExecutionContext): Future[NettyChannelBuilder] =
    Future(clientChannelConfig.builderFor(ledgerHost, ledgerPort))

}
