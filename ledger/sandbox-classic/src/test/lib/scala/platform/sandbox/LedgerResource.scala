// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.StandardTransactionCommitter
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.InMemoryActiveLedgerState
import com.daml.platform.sandbox.stores.ledger.Ledger
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.daml.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.LfValueTranslationCache
import com.daml.testing.postgresql.PostgresResource

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

private[sandbox] object LedgerResource {

  def inMemory(
      ledgerId: LedgerId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState = InMemoryActiveLedgerState.empty,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
      entries: ImmArray[LedgerEntryOrBump] = ImmArray.Empty,
  )(implicit resourceContext: ResourceContext): Resource[Ledger] =
    new OwnedResource(
      ResourceOwner.forValue(() =>
        new InMemoryLedger(
          ledgerId = ledgerId,
          timeProvider = timeProvider,
          acs0 = acs,
          transactionCommitter = StandardTransactionCommitter,
          packageStoreInit = packages,
          ledgerEntries = entries,
          engine = new Engine(),
        )
      )
    )

  private val TestParticipantId =
    domain.ParticipantId(Ref.ParticipantId.assertFromString("test-participant-id"))

  def postgres(
      testClass: Class[_],
      ledgerId: LedgerId,
      timeProvider: TimeProvider,
      metrics: MetricRegistry,
      errorFactories: ErrorFactories,
      packages: InMemoryPackageStore = InMemoryPackageStore.empty,
  )(implicit
      resourceContext: ResourceContext,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): Resource[Ledger] =
    new OwnedResource(
      for {
        servicesExecutionContext <- ResourceOwner
          .forExecutorService(() => Executors.newWorkStealingPool())
          .map(ExecutionContext.fromExecutorService)
        database <- PostgresResource.owner[ResourceContext]()
        ledger <- new SqlLedger.Owner(
          name = LedgerName(testClass.getSimpleName),
          serverRole = ServerRole.Testing(testClass),
          jdbcUrl = database.url,
          databaseConnectionPoolSize = 16,
          databaseConnectionTimeout = 250.millis,
          providedLedgerId = LedgerIdMode.Static(ledgerId),
          participantId = TestParticipantId,
          timeProvider = timeProvider,
          packages = packages,
          initialLedgerEntries = ImmArray.Empty,
          queueDepth = 128,
          transactionCommitter = StandardTransactionCommitter,
          startMode = SqlStartMode.ResetAndStart,
          eventsPageSize = 100,
          eventsProcessingParallelism = 8,
          servicesExecutionContext = servicesExecutionContext,
          metrics = new Metrics(metrics),
          lfValueTranslationCache = LfValueTranslationCache.Cache.none,
          engine = new Engine(),
          validatePartyAllocation = false,
          enableCompression = false,
          errorFactories = errorFactories,
        )
      } yield ledger
    )
}
