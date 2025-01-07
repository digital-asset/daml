// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.topology.PartyId

/** Serializable contract with witnesses for contract add/import used in admin repairs.
  *
  * @param contract serializable contract
  * @param witnesses witnesses that observe the creation of the contract
  * @param reassignmentCounter reassignment counter for the given [[contract]]
  */
final case class RepairContract(
    contract: SerializableContract,
    witnesses: Set[PartyId],
    reassignmentCounter: ReassignmentCounter,
)
