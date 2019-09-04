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
  * @param divulgedTo:
  *    The optional set of parties to which the contract has been divulged to. If not set, then
  *    the set of parties this contract is revealed to is derived from the associated transaction.
  *    This primary purpose of this field is to allow synthetic divulgence after a ledger pruning event.
  */
final case class DivulgedContract(
    contractId: Value.AbsoluteContractId,
    contractInst: AbsoluteContractInst,
    divulgedTo: Option[Set[Party]]
)
