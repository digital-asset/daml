// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.traffic

import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Configuration for the traffic balance manager.
  * @param trafficBalanceCacheSizePerMember How many traffic balances to keep in memory for each member.
  * @param maximumTrafficBalanceCacheSize The maximum number of entries (= members) to keep in the cache.
  */
final case class SequencerTrafficConfig(
    trafficBalanceCacheSizePerMember: PositiveInt = PositiveInt.tryCreate(3),
    maximumTrafficBalanceCacheSize: PositiveInt = PositiveInt.tryCreate(1000),
)
