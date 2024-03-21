// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import akka.stream.Materializer
import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.index.IndexServiceBuilder
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}

import scala.concurrent.ExecutionContextExecutor

object StandaloneIndexService {
  def apply(
      dbSupport: DbSupport,
      ledgerId: LedgerId,
      config: IndexServiceConfig,
      participantId: Ref.ParticipantId,
      metrics: Metrics,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      // TODO LLP: Always pass shared stringInterningView
      sharedStringInterningViewO: Option[StringInterningView] = None,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexService] =
    for {
      indexService <- IndexServiceBuilder(
        dbSupport = dbSupport,
        config = config,
        initialLedgerId = domain.LedgerId(ledgerId),
        participantId = participantId,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = lfValueTranslationCache,
        enricher = new ValueEnricher(engine),
        sharedStringInterningViewO = sharedStringInterningViewO,
      )(materializer, loggingContext, servicesExecutionContext)
        .owner()
        .map(index => new TimedIndexService(index, metrics))
    } yield indexService
}
