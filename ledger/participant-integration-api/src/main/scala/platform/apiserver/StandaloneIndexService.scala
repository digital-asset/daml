// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

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
import com.daml.platform.index.ReadOnlyLedgerBuilder
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}

import java.io.File
import scala.concurrent.ExecutionContextExecutor

object StandaloneIndexService {
  def apply(
      dbSupport: DbSupport,
      ledgerId: LedgerId,
      config: ApiServerConfig,
      metrics: Metrics,
      engine: Engine,
      servicesExecutionContext: ExecutionContextExecutor,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
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
      indexService <- ReadOnlyLedgerBuilder(
        dbSupport = dbSupport,
        initialLedgerId = domain.LedgerId(ledgerId),
        participantId = participantId,
        eventsPageSize = config.eventsPageSize,
        eventsProcessingParallelism = config.eventsProcessingParallelism,
        acsIdPageSize = config.acsIdPageSize,
        acsIdFetchingParallelism = config.acsIdFetchingParallelism,
        acsContractFetchingParallelism = config.acsContractFetchingParallelism,
        acsGlobalParallelism = config.acsGlobalParallelism,
        acsIdQueueLimit = config.acsIdQueueLimit,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = lfValueTranslationCache,
        enricher = valueEnricher,
        maxContractStateCacheSize = config.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = config.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
        enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
        errorFactories = ErrorFactories(),
      )(materializer, loggingContext, servicesExecutionContext)
        .owner()
        .map(index => new TimedIndexService(index, metrics))
    } yield indexService
  }
}
