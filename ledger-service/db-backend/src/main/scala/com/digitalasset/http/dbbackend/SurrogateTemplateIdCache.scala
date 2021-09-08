// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import com.codahale.metrics.MetricRegistry
import com.daml.caching.SizedCache
import com.daml.http.dbbackend.Queries.SurrogateTpId
import com.daml.metrics.Metrics
import org.slf4j.LoggerFactory

object SurrogateTemplateIdCache {
  private val logger = LoggerFactory.getLogger(classOf[SurrogateTemplateIdCache])
  private val maxSize = 1000L
}

trait SurrogateTemplateIdCache {
  import SurrogateTemplateIdCache.{logger, maxSize}

  private val metrics = new Metrics(new MetricRegistry)

  private val underlying = {
    SizedCache.from[String, java.lang.Long](
      SizedCache.Configuration(maxSize),
      metrics.daml.HttpJsonApi.surrogateTemplateIdCache,
    )
  }

  final def tpIdCachedValue(packageId: String, moduleName: String, entityName: String) = {
    val key = s"$packageId-$moduleName-$entityName"
    val res = underlying.getIfPresent(key).map(x => SurrogateTpId(x.toLong))
    logger.trace(s"Fetched cached value for key ($key) : $res")
    res
  }

  final def setTpIdCacheValue(
      packageId: String,
      moduleName: String,
      entityName: String,
      tpId: SurrogateTpId,
  ) = {
    val key = s"$packageId-$moduleName-$entityName"
    logger.trace(s"Set cached value for key ($key) : $tpId")
    underlying.put(key, SurrogateTpId.unwrap(tpId))
  }

  //for testing purposes.
  import metrics.daml.HttpJsonApi.{surrogateTemplateIdCache => cacheStats}
  final def tpIdCacheHits = cacheStats.hitCount.getCount
  final def tpIdCacheMiss = cacheStats.missCount.getCount

}
