// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.resources.ResourceOwner

object JdbcIndex {
  def owner(
      serverRole: ServerRole,
      initialConfig: Configuration,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      eventsPageSize: Int,
      metrics: Metrics,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[IndexService] =
    ReadOnlySqlLedger
      .owner(serverRole, jdbcUrl, ledgerId, eventsPageSize, metrics)
      .map { ledger =>
        new LedgerBackedIndexService(MeteredReadOnlyLedger(ledger, metrics), participantId) {
          override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
            // FIXME(JM): The indexer should on start set the default configuration.
            Source.single(v2.LedgerConfiguration(initialConfig.maxDeduplicationTime))
        }
      }
}
