// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService}
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.store.ActiveLedgerStateManager.IndexingOptions
import com.digitalasset.resources.Resource

object JdbcIndex {
  def apply(
      readService: ReadService,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      metrics: MetricRegistry
  )(
      implicit mat: Materializer,
      logCtx: LoggingContext,
      indexingOptions: IndexingOptions = IndexingOptions.defaultNoImplicitPartyAllocation)
    : Resource[IndexService] =
    ReadOnlySqlLedger(jdbcUrl, Some(ledgerId), metrics).map { ledger =>
      new LedgerBackedIndexService(MeteredReadOnlyLedger(ledger, metrics), participantId) {
        override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
          // FIXME(JM): This is broken. We should not use ReadService in Ledger API Server,
          // The indexer should on start set the default configuration.
          readService.getLedgerInitialConditions().map { cond =>
            v2.LedgerConfiguration(cond.config.timeModel.minTtl, cond.config.timeModel.maxTtl)
          }
      }
    }(DirectExecutionContext)
}
