// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.{OwnedResource, Resource}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.digitalasset.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresResource

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
      eventsPageSize: Int,
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
          metrics = metrics,
          eventsPageSize = eventsPageSize,
        )
      } yield ledger
    )
}
