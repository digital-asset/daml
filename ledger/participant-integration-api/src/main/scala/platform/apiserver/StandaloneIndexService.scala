// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.io.File

import akka.stream.Materializer
import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.index.JdbcIndex
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.store.LfValueTranslationCache

import scala.concurrent.ExecutionContextExecutor

object StandaloneIndexService {
  def apply(
      ledgerId: LedgerId,
      config: ApiServerConfig,
      metrics: Metrics,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      ledgerEndUpdateChannel: Option[LooseSyncChannel] = None,
  )(implicit
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexService] = {
    val participantId: Ref.ParticipantId = config.participantId
    val valueEnricher = new ValueEnricher(engine)

    def preloadPackages(packageContainer: InMemoryPackageStore): Unit = {
      for {
        (pkgId, _) <- packageContainer.listLfPackagesSync()
        pkg <- packageContainer.getLfPackageSync(pkgId)
      } {
        engine
          .preloadPackage(pkgId, pkg)
          .consume(
            { _ =>
              sys.error("Unexpected request of contract")
            },
            packageContainer.getLfPackageSync,
            { _ =>
              sys.error("Unexpected request of contract key")
            },
          )
        ()
      }
    }

    def loadDamlPackages(): InMemoryPackageStore = {
      // TODO is it sensible to have all the initial packages to be known since the epoch?
      config.archiveFiles
        .foldLeft[Either[(String, File), InMemoryPackageStore]](Right(InMemoryPackageStore.empty)) {
          case (storeE, f) =>
            storeE.flatMap(_.withDarFile(Timestamp.now(), None, f).left.map(_ -> f))
        }
        .fold({ case (err, file) => sys.error(s"Could not load package $file: $err") }, identity)
    }

    for {
      _ <- ResourceOwner.forValue(() => {
        val packageStore = loadDamlPackages()
        preloadPackages(packageStore)
      })
      indexService <- JdbcIndex
        .owner(
          serverRole = ServerRole.ApiServer,
          ledgerId = domain.LedgerId(ledgerId),
          participantId = participantId,
          jdbcUrl = config.jdbcUrl,
          databaseConnectionPoolSize = config.databaseConnectionPoolSize,
          databaseConnectionTimeout = config.databaseConnectionTimeout,
          eventsPageSize = config.eventsPageSize,
          eventsProcessingParallelism = config.eventsProcessingParallelism,
          acsIdPageSize = config.acsIdPageSize,
          acsIdFetchingParallelism = config.acsIdFetchingParallelism,
          acsContractFetchingParallelism = config.acsContractFetchingParallelism,
          servicesExecutionContext = servicesExecutionContext,
          metrics = metrics,
          lfValueTranslationCache = lfValueTranslationCache,
          enricher = valueEnricher,
          maxContractStateCacheSize = config.maxContractStateCacheSize,
          maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
          enableMutableContractStateCache = config.enableMutableContractStateCache,
          maxTransactionsInMemoryFanOutBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
          enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
          enableSelfServiceErrorCodes = config.enableSelfServiceErrorCodes,
          ledgerEndUpdateChannel = ledgerEndUpdateChannel,
        )
        .map(index => new SpannedIndexService(new TimedIndexService(index, metrics)))
    } yield indexService
  }
}
