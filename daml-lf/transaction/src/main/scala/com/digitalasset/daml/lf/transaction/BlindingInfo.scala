// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.value.Value.ContractId

/** A transaction's blinding information, consisting of disclosure and
  * divulgence info.
  *
  * "Disclosure" tells us which transaction nodes to communicate to which
  * parties.
  * See https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
  *
  * "Divulgence" tells us which contracts to communicate to which parties
  * so that their participant nodes can perform post-commit validation.
  * Note that divulgence can also divulge e.g. contract IDs that were
  * created _outside_ this transaction.
  * See also https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  *
  * @param disclosure Disclosure, specified in terms of local transaction node IDs
  * @param divulgence
  *     Divulgence, specified in terms of contract IDs.
  *     Note that if this info was produced by blinding a transaction
  *     containing only contract ids, this map may also
  *     contain contracts produced in the same transaction.
  */
final case class BlindingInfo(
    disclosure: Relation[NodeId, Party],
    divulgence: Relation[ContractId, Party],
)
