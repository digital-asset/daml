// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v2

import com.daml.lf.value.Value

/** A divulged contract, that is, a contract that has been revealed to a non-stakeholder
  * after its creation.
  * For more information on divulgence, see:
  * https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  *
  * @param contractId: The contract identifier.
  * @param contractInst: The contract instance.
  */
final case class DivulgedContract(
    contractId: Value.ContractId,
    contractInst: Value.VersionedContractInstance,
)
