// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService}
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{ParticipantId => _, _}
import com.digitalasset.platform.logging.LoggingContext
import com.digitalasset.platform.resources.Resource
import com.digitalasset.platform.sandbox.stores.LedgerBackedIndexService
import com.digitalasset.platform.sandbox.stores.ledger.{
  Ledger,
  MeteredReadOnlyLedger,
  SandboxContractStore
}

object JdbcIndex {
  def apply(
      readService: ReadService,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, ctx: LoggingContext): Resource[IndexService] =
    Ledger
      .jdbcBackedReadOnly(jdbcUrl, ledgerId, metrics)
      .map { ledger =>
        val contractStore = new SandboxContractStore(ledger)
        new LedgerBackedIndexService(
          MeteredReadOnlyLedger(ledger, metrics),
          contractStore,
          participantId) {
          override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
            // FIXME(JM): This is broken. We should not use ReadService in Ledger API Server,
            // The indexer should on start set the default configuration.
            readService.getLedgerInitialConditions().map { cond =>
              v2.LedgerConfiguration(cond.config.timeModel.minTtl, cond.config.timeModel.maxTtl)
            }
        }
      }(DirectExecutionContext)
      .vary
}
