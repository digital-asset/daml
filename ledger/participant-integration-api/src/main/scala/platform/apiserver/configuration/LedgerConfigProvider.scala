// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.LedgerConfiguration

import scala.concurrent.ExecutionContext

object LedgerConfigProvider {
  def owner(
      ledgerConfiguration: LedgerConfiguration,
      index: IndexConfigManagementService,
      optWriteService: Option[state.WriteConfigService],
      timeProvider: TimeProvider,
      servicesExecutionContext: ExecutionContext,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[CurrentLedgerConfiguration] = {
    val scheduler = materializer.system.scheduler
    for {
      // First, we acquire the mechanism for looking up the current ledger configuration.
      currentLedgerConfiguration <- IndexStreamingCurrentLedgerConfiguration.owner(
        ledgerConfiguration = ledgerConfiguration,
        index = index,
        scheduler = scheduler,
        materializer = materializer,
        servicesExecutionContext = servicesExecutionContext,
      )
      // Next, we provision the configuration if one does not already exist on the ledger.
      _ <- optWriteService match {
        case None => ResourceOwner.unit
        case Some(writeService) =>
          LedgerConfigProvisioner.owner(
            ledgerConfiguration = ledgerConfiguration,
            currentLedgerConfiguration = currentLedgerConfiguration,
            writeService = writeService,
            timeProvider = timeProvider,
            submissionIdGenerator = SubmissionIdGenerator.Random,
            scheduler = scheduler,
            servicesExecutionContext = servicesExecutionContext,
          )
      }
      // Finally, we wait until either an existing configuration or the provisioned configuration
      // appears on the index.
      _ <- ResourceOwner.forFuture(() => currentLedgerConfiguration.ready)
    } yield currentLedgerConfiguration
  }
}
