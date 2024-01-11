// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.LanguageVersion
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.caching.CaffeineCache
import com.digitalasset.canton.caching.CaffeineCache.FutureAsyncCacheLoader
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.dao.LedgerReadDao
import com.digitalasset.canton.tracing.TraceContext
import com.github.benmanes.caffeine.cache as caffeine

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class PackageLanguageVersionCache(
    ledgerDao: LedgerReadDao,
    metrics: Metrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val languageVersionLoader =
    new FutureAsyncCacheLoader[PackageId, Option[LanguageVersion]](packageId =>
      getLanguageVersion(packageId)(LoggingContextWithTrace(loggerFactory)(TraceContext.empty))
    )

  private val languageVersionCache
      : CaffeineCache.AsyncLoadingCaffeineCache[PackageId, Option[LanguageVersion]] =
    new CaffeineCache.AsyncLoadingCaffeineCache[PackageId, Option[LanguageVersion]](
      caffeine.Caffeine
        .newBuilder()
        .maximumSize(10000)
        .buildAsync(languageVersionLoader),
      metrics.daml.index.packageLanguageVersionCache,
    )

  private def getLanguageVersion(
      packageId: PackageId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[LanguageVersion]] = {
    ledgerDao
      .getLfArchive(packageId)
      .map({ archiveO =>
        archiveO.map { archive =>
          val (_, pkg) = Decode.assertDecodeArchive(archive)
          logger.debug(s"Caching package language version for $packageId: ${pkg.languageVersion}")(
            TraceContext.empty
          )
          pkg.languageVersion
        }
      })
  }

  def get(packageId: PackageId): Future[Option[LanguageVersion]] = {
    val result = languageVersionCache.get(packageId)
    // Do not cache failed look-ups as the package may be loaded before the next call
    result
      .andThen({ case Success(None) => languageVersionCache.invalidate(packageId) })
      .discard
    result
  }

}
