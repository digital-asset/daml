// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.LedgerConfiguration

object LedgerConfigProvider {
  def owner(
      index: IndexConfigManagementService,
      optWriteService: Option[state.WriteConfigService],
      timeProvider: TimeProvider,
      config: LedgerConfiguration,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[CurrentLedgerConfiguration] =
    for {
      provider <- CurrentLedgerConfiguration.owner(index, config)
      _ <- optWriteService match {
        case None => ResourceOwner.unit
        case Some(writeService) =>
          LedgerConfigProvisioner.owner(provider, writeService, timeProvider, config)
      }
      _ <- ResourceOwner.forFuture(() => provider.ready)
    } yield provider
}
