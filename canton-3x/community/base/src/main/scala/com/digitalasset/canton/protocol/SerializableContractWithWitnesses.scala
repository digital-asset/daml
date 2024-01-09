// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.RepairContract
import com.digitalasset.canton.topology.PartyId

/*
  Allows backward compatibility for user scripts
  TODO(#14441) Remove this object
 */
object SerializableContractWithWitnesses {
  def apply(contract: SerializableContract, witnesses: Set[PartyId]): RepairContract =
    RepairContract(contract, witnesses, None)
}
