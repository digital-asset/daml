// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Ledger API package service specific configurations
  *
  * @param maxVettedPackagesPageSize
  *   maximum number of VettedPackages results returned
  */
final case class PackageServiceConfig(
    maxVettedPackagesPageSize: PositiveInt = PackageServiceConfig.DefaultMaxVettedPackagesPageSize
)

object PackageServiceConfig {

  val DefaultMaxVettedPackagesPageSize: PositiveInt = PositiveInt.tryCreate(100)

  def default: PackageServiceConfig = PackageServiceConfig(
    maxVettedPackagesPageSize = DefaultMaxVettedPackagesPageSize
  )
}
