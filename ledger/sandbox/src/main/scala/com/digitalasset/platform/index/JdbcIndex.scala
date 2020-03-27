// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, TimeModel}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.resources.ResourceOwner

object JdbcIndex {
  def owner(
      serverRole: ServerRole,
      timeModel: TimeModel,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[IndexService] =
    ReadOnlySqlLedger
      .owner(serverRole, jdbcUrl, ledgerId, metrics)
      .map { ledger =>
        new LedgerBackedIndexService(MeteredReadOnlyLedger(ledger, metrics), participantId) {
          override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
            // FIXME(JM): The indexer should on start set the default configuration.
            Source.single(v2.LedgerConfiguration(timeModel.minTtl, timeModel.maxTtl))
        }
      }
}
