// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.configuration

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.SubmissionIdGenerator
import com.daml.ledger.participant.state.index.v2.IndexConfigManagementService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.platform.configuration.InitialLedgerConfiguration

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

final class LedgerConfigurationInitializer(
    indexService: IndexConfigManagementService,
    optWriteService: Option[state.WriteConfigService],
    timeProvider: TimeProvider,
    materializer: Materializer,
    servicesExecutionContext: ExecutionContext,
) {
  private val scheduler = materializer.system.scheduler
  private val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
    indexService,
    scheduler,
    materializer,
    servicesExecutionContext,
  )

  def initialize(
      initialLedgerConfiguration: InitialLedgerConfiguration,
      configurationLoadTimeout: Duration,
  )(implicit
      resourceContext: ResourceContext,
      loggingContext: LoggingContext,
  ): Resource[LedgerConfigurationSubscription] = {
    val owner = for {
      // First, we acquire the mechanism for looking up the current ledger configuration.
      ledgerConfigurationSubscription <- subscriptionBuilder.subscription(configurationLoadTimeout)
      // Next, we provision the configuration if one does not already exist on the ledger.
      _ <- optWriteService match {
        case None => ResourceOwner.unit
        case Some(writeService) =>
          val submissionIdGenerator = SubmissionIdGenerator.Random
          new LedgerConfigurationProvisioner(
            ledgerConfigurationSubscription,
            writeService,
            timeProvider,
            submissionIdGenerator,
            scheduler,
          ).submit(initialLedgerConfiguration)(servicesExecutionContext, loggingContext)
      }
      // Finally, we wait until either an existing configuration or the provisioned configuration
      // appears on the index.
      _ <- ResourceOwner.forFuture(() => ledgerConfigurationSubscription.ready)
    } yield ledgerConfigurationSubscription
    owner.acquire()
  }
}
