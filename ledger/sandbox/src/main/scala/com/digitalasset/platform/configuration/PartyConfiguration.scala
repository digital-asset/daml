// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.configuration

case class PartyConfiguration(implicitPartyAllocation: Boolean)

object PartyConfiguration {
  val default: PartyConfiguration = PartyConfiguration(
    implicitPartyAllocation = false,
  )
}
