// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}

/** Ledger api party management service specific configurations
  *
  * @param maxPartiesPageSize
  *   maximum number of parties returned
  */
final case class PartyManagementServiceConfig(
    maxPartiesPageSize: PositiveInt = PartyManagementServiceConfig.DefaultMaxPartiesPageSize,
    maxSelfAllocatedParties: NonNegativeInt =
      PartyManagementServiceConfig.DefaultMaxSelfAllocatedParties,
)

object PartyManagementServiceConfig {

  val DefaultMaxPartiesPageSize: PositiveInt = PositiveInt.tryCreate(10000)
  val DefaultMaxSelfAllocatedParties: NonNegativeInt = NonNegativeInt.tryCreate(0)

  def default: PartyManagementServiceConfig = PartyManagementServiceConfig(
    maxPartiesPageSize = DefaultMaxPartiesPageSize,
    maxSelfAllocatedParties = DefaultMaxSelfAllocatedParties,
  )
}
