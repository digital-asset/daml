// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v1

import com.digitalasset.daml.lf.value.Value

/** A divulged contract, that is, a contract that has been revealed to a non-stakeholder
  * after its creation.
  * For more information on divulgence, see:
  * https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  *
  * @param contractId: The absolute contract identifier.
  * @param contractInst: The contract instance.
  */
final case class DivulgedContract(
    contractId: Value.AbsoluteContractId,
    contractInst: AbsoluteContractInst
)
