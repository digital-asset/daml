// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric

/** Parameters for the in-memory cache that can be used in front of a db-based
  * data provider.
  *
  * @param maximumCacheSize Maximum number of elements of the cache.
  */
final case class DbCacheConfig(
    maximumCacheSize: PositiveNumeric[Long] = DbCacheConfig.defaultMaximumCacheSize
)

object DbCacheConfig {
  private[DbCacheConfig] val defaultMaximumCacheSize: PositiveNumeric[Long] =
    PositiveNumeric.tryCreate(1000000)

  def defaultsForTesting() = DbCacheConfig(maximumCacheSize = PositiveNumeric.tryCreate(100L))
}
