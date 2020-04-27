// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.daml.api.util.TimeProvider
import com.daml.lf.data.ImmArray
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.logging.LoggingContext
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.daml.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.daml.resources.ResourceOwner
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.ExecutionContext

object LedgerResource {
  def inMemory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      initialConfig: Configuration,
      acs: InMemoryActiveLedgerState = InMemoryActiveLedgerState.empty,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
      entries: ImmArray[LedgerEntryOrBump] = ImmArray.empty,
  )(implicit executionContext: ExecutionContext): Resource[Ledger] =
    new OwnedResource(
      ResourceOwner.successful(
        new InMemoryLedger(
          ledgerId,
          participantId,
          timeProvider,
          acs,
          packages,
          entries,
          initialConfig)))

  def postgres(
      testClass: Class[_],
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      initialConfig: Configuration,
      metrics: MetricRegistry,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): Resource[Ledger] =
    new OwnedResource(
      for {
        postgres <- PostgresResource.owner()
        ledger <- SqlLedger.owner(
          serverRole = ServerRole.Testing(testClass),
          jdbcUrl = postgres.jdbcUrl,
          ledgerId = LedgerIdMode.Static(ledgerId),
          participantId = participantId,
          timeProvider = timeProvider,
          acs = InMemoryActiveLedgerState.empty,
          packages = packages,
          initialLedgerEntries = ImmArray.empty,
          initialConfig = initialConfig,
          queueDepth = 128,
          startMode = SqlStartMode.AlwaysReset,
          eventsPageSize = 100,
          metrics = metrics,
        )
      } yield ledger
    )
}
