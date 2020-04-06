// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v1

import com.daml.lf.value.Value

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
