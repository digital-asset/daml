// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

/** Ledger api party management service specific configurations
  *
  * @param maxPartiesPageSize               maximum number of users returned
  */
final case class PartyManagementServiceConfig(
    maxPartiesPageSize: Int = PartyManagementServiceConfig.DefaultMaxPartiesPageSize
)

object PartyManagementServiceConfig {

  val DefaultMaxPartiesPageSize = 1000

  def default: PartyManagementServiceConfig = PartyManagementServiceConfig(
    maxPartiesPageSize = DefaultMaxPartiesPageSize
  )
}
