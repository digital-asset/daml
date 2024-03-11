// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.lf.archive.Decode
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref.PackageId
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

// TODO(#17714) Replace use with PackageMetadataView
class KeyPackageNameCache(
    ledgerDao: LedgerReadDao,
    metrics: Metrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val keyPackageNameLoader =
    new FutureAsyncCacheLoader[PackageId, Option[KeyPackageName]](packageId =>
      getKeyPackageName(packageId)(LoggingContextWithTrace(loggerFactory)(TraceContext.empty))
    )

  private val keyPackageNameCache
      : CaffeineCache.AsyncLoadingCaffeineCache[PackageId, Option[KeyPackageName]] =
    new CaffeineCache.AsyncLoadingCaffeineCache[PackageId, Option[KeyPackageName]](
      caffeine.Caffeine
        .newBuilder()
        .maximumSize(10000)
        .buildAsync(keyPackageNameLoader),
      metrics.daml.index.keyPackageNameCache,
    )

  private def getKeyPackageName(
      packageId: PackageId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[KeyPackageName]] = {
    ledgerDao
      .getLfArchive(packageId)
      .map({ archiveO =>
        archiveO.map { archive =>
          val (_, pkg) = Decode.assertDecodeArchive(archive)
          logger.debug(s"Caching key package name for package $packageId")(
            TraceContext.empty
          )
          KeyPackageName.assertBuild(pkg.metadata.map(_.name), pkg.languageVersion)
        }
      })
  }

  def get(packageId: PackageId): Future[Option[KeyPackageName]] = {
    val result = keyPackageNameCache.get(packageId)
    // Do not cache failed look-ups as the package may be loaded before the next call
    result
      .andThen({ case Success(None) => keyPackageNameCache.invalidate(packageId) })
      .discard
    result
  }

}
