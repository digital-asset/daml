// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.TransferCounterO
import com.digitalasset.canton.protocol.SerializableContract
import com.digitalasset.canton.topology.PartyId

/** Serializable contract with witnesses for contract add/import used in admin repairs.
  *
  * @param contract serializable contract
  * @param witnesses optional witnesses that observe the creation of the contract
  * @param transferCounter optional reassignment counter for the given [[contract]]
  */
final case class RepairContract(
    contract: SerializableContract,
    witnesses: Set[PartyId],
    transferCounter: TransferCounterO,
)
