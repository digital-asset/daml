// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.daml.caching.SizedCache
import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.http.util.Logging.InstanceUUID
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics

object SurrogateTemplateIdCache {
  private val logger = ContextualizedLogger.get(getClass)
  final val MaxEntries = 10L
}

class SurrogateTemplateIdCache(metrics: Metrics, maxEntries: Long) {
  import SurrogateTemplateIdCache.logger

  private val underlying = {
    SizedCache.from[String, java.lang.Long](
      SizedCache.Configuration(maxEntries),
      metrics.daml.HttpJsonApi.surrogateTemplateIdCache,
    )
  }

  final def getCacheValue(packageId: String, moduleName: String, entityName: String)(implicit
      lc: LoggingContextOf[InstanceUUID]
  ) = {
    val key = s"$packageId-$moduleName-$entityName"
    val res = underlying.getIfPresent(key).map(x => SurrogateTpId(x.toLong))
    logger.trace(s"Fetched cached value for key ($key) : $res")
    res
  }

  final def setCacheValue(
      packageId: String,
      moduleName: String,
      entityName: String,
      tpId: SurrogateTpId,
  )(implicit lc: LoggingContextOf[InstanceUUID]) = {
    val key = s"$packageId-$moduleName-$entityName"
    logger.trace(s"Set cached value for key ($key) : $tpId")
    underlying.put(key, SurrogateTpId.unwrap(tpId))
  }

  // for testing purposes.
  import metrics.daml.HttpJsonApi.{surrogateTemplateIdCache => cacheStats}
  private[http] final def getHitCount = cacheStats.hitCount.getCount
  private[http] final def getMissCount = cacheStats.missCount.getCount

}
