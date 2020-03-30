// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.resources.ResourceOwner

object JdbcIndex {
  def owner(
      serverRole: ServerRole,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[IndexService] =
    ReadOnlySqlLedger
      .owner(serverRole, jdbcUrl, ledgerId, metrics)
      .map { ledger =>
        new LedgerBackedIndexService(MeteredReadOnlyLedger(ledger, metrics), participantId)
      }
}
