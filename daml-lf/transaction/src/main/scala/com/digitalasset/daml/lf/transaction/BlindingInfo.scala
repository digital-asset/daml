// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

/** This gives disclosure and divulgence info.
  *
  * "Disclosure" tells us which nodes to communicate to which parties.
  * See also https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
  *
  * "Divulgence" tells us what to communicate to
  * each participant node so that they can perform post-commit
  * validation. Note that divulgence can also divulge
  * absolute contract ids -- e.g. contract ids that were created
  * _outside_ this transaction.
  * See also https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  */
case class BlindingInfo(
    /** Disclosure, specified in terms of local node IDs */
    disclosure: Relation[Transaction.NodeId, Party],
    /** Divulgence, specified in terms of local node IDs */
    localDivulgence: Relation[Transaction.NodeId, Party],
    /**
      * Divulgence, specified in terms of absolute contract IDs.
      * Note that if this info was produced by blinding a transaction
      * containing only absolute contract ids, this map may also
      * contain contracts produced in the same transaction.
      */
    globalDivulgence: Relation[AbsoluteContractId, Party],
) {
  def localDisclosure: Relation[Transaction.NodeId, Party] =
    Relation.union(disclosure, localDivulgence)
}
