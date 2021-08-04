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
import com.daml.platform.configuration.InitialLedgerConfiguration

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

object LedgerConfigurationInitializer {
  def owner(
      initialLedgerConfiguration: InitialLedgerConfiguration,
      configurationLoadTimeout: Duration,
      indexService: IndexConfigManagementService,
      optWriteService: Option[state.WriteConfigService],
      timeProvider: TimeProvider,
      servicesExecutionContext: ExecutionContext,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[LedgerConfigurationSubscription] = {
    val scheduler = materializer.system.scheduler
    for {
      // First, we acquire the mechanism for looking up the current ledger configuration.
      ledgerConfigurationSubscription <- LedgerConfigurationIndexSubscription.owner(
        configurationLoadTimeout,
        indexService,
        scheduler,
        materializer,
        servicesExecutionContext,
      )
      // Next, we provision the configuration if one does not already exist on the ledger.
      _ <- optWriteService match {
        case None => ResourceOwner.unit
        case Some(writeService) =>
          val submissionIdGenerator = SubmissionIdGenerator.Random
          LedgerConfigurationProvisioner.owner(
            initialLedgerConfiguration,
            ledgerConfigurationSubscription,
            writeService,
            timeProvider,
            submissionIdGenerator,
            scheduler,
            servicesExecutionContext,
          )
      }
      // Finally, we wait until either an existing configuration or the provisioned configuration
      // appears on the index.
      _ <- ResourceOwner.forFuture(() => ledgerConfigurationSubscription.ready)
    } yield ledgerConfigurationSubscription
  }
}
