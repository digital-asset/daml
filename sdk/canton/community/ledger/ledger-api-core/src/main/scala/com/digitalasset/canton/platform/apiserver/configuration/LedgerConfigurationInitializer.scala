// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.configuration

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.ledger.participant.state.index.v2.IndexConfigManagementService
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.Materializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

final class LedgerConfigurationInitializer(
    indexService: IndexConfigManagementService,
    materializer: Materializer,
    servicesExecutionContext: ExecutionContext,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val scheduler = materializer.system.scheduler
  private val subscriptionBuilder = new LedgerConfigurationSubscriptionFromIndex(
    indexService,
    scheduler,
    materializer,
    servicesExecutionContext,
    loggerFactory,
  )

  def initialize(
      configurationLoadTimeout: Duration
  )(implicit
      resourceContext: ResourceContext
  ): Resource[LedgerConfigurationSubscription] = {
    implicit val loggingContext = LoggingContextWithTrace(loggerFactory)(TraceContext.empty)
    val owner = for {
      // Acquire the mechanism for looking up the current ledger configuration.
      ledgerConfigurationSubscription <- subscriptionBuilder.subscription(configurationLoadTimeout)
      // Wait until an existing configuration appears on the index.
      _ <- ResourceOwner.forFuture(() => ledgerConfigurationSubscription.ready)
    } yield ledgerConfigurationSubscription
    owner.acquire()
  }
}
