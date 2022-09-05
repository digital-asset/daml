// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

/** Configuration surrounding parties and party allocation.
  *
  * @param implicitPartyAllocation
  *     Informs the Command Submission Service that any parties mentioned in a submission that do
  *     not already exist should be created on the fly. This behavior will not make sense for most
  *     ledgers, because:
  *       - allocating parties can be racy if multiple transactions are coming through at once,
  *       - if allocation fails, the errors are a little confusing, and
  *       - parties can be allocated without administrator privileges.
  *
  *     Enable at your own risk.
  */
case class PartyConfiguration(
    implicitPartyAllocation: Boolean = false
)

object PartyConfiguration {
  val Default: PartyConfiguration = PartyConfiguration()
}
