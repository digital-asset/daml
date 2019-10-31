// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, ReadService}
import com.digitalasset.ledger.api.domain.{ParticipantId => _, _}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.LedgerBackedIndexService
import com.digitalasset.platform.sandbox.stores.ledger.{
  Ledger,
  MeteredReadOnlyLedger,
  SandboxContractStore
}

import scala.concurrent.Future

object JdbcIndex {
  def apply(
      readService: ReadService,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      loggerFactory: NamedLoggerFactory,
      mm: MetricsManager)(implicit mat: Materializer): Future[IndexService with AutoCloseable] =
    Ledger
      .jdbcBackedReadOnly(jdbcUrl, ledgerId, loggerFactory, mm)
      .map { ledger =>
        val contractStore = new SandboxContractStore(ledger)
        new LedgerBackedIndexService(
          MeteredReadOnlyLedger(ledger, mm),
          contractStore,
          participantId) {
          override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
            readService.getLedgerInitialConditions().map { cond =>
              v2.LedgerConfiguration(cond.config.timeModel.minTtl, cond.config.timeModel.maxTtl)
            }
        }
      }(DEC)
}
