// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{BatchAggregatorConfig, NonNegativeFiniteDuration}

/** Configuration for the traffic purchased entry manager.
  * @param trafficPurchasedCacheSizePerMember How many traffic purchased entries to keep in memory for each member.
  * @param maximumTrafficPurchasedCacheSize The maximum number of entries (= members) to keep in the cache.
  * @param batchAggregatorConfig configures how balances are batched before being written to the store.
  * @param pruningRetentionWindow the duration for which balances are kept in the cache and the store.
  *        Balances older than this duration will be pruned at regular intervals.
  * @param trafficConsumedCacheTTL the duration for which consumed traffic entries are kept in the cache after the last time they've been accessed.
  * @param maximumTrafficConsumedCacheSize Maximum number of entries (members) to keep in the traffic consumed cache.
  * @param submissionTimestampInFutureTolerance the tolerance window that should be added to future dated submission timestamp that can still be accepted by this sequencer.
  */
final case class SequencerTrafficConfig(
    trafficPurchasedCacheSizePerMember: PositiveInt = PositiveInt.tryCreate(3),
    maximumTrafficPurchasedCacheSize: PositiveInt = PositiveInt.tryCreate(1000),
    batchAggregatorConfig: BatchAggregatorConfig = BatchAggregatorConfig.Batching(),
    pruningRetentionWindow: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(2L),
    trafficConsumedCacheTTL: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(2L),
    maximumTrafficConsumedCacheSize: PositiveInt = PositiveInt.tryCreate(1000),
    submissionTimestampInFutureTolerance: NonNegativeFiniteDuration =
      NonNegativeFiniteDuration.ofSeconds(5),
)
