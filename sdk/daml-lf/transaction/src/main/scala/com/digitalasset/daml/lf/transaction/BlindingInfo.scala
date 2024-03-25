// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation
import com.daml.lf.value.Value.ContractId

/** A transaction's blinding information, consisting of disclosure and
  * divulgence info.
  *
  * "Disclosure" tells us which transaction nodes to communicate to which
  * parties.
  * See https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
  * Note that rollback nodes are a bit special here: Even if the node
  * itself is not disclosed, a party will be disclosed if a node was below a
  * rollback node. That information is not included in blinding info at this point.
  *
  * "Divulgence" tells us which contracts to communicate to which parties
  * so that their participant nodes can perform post-commit validation.
  * Note that divulgence can also divulge e.g. contract IDs that were
  * created _outside_ this transaction.
  * See also https://docs.daml.com/concepts/ledger-model/ledger-privacy.html#divulgence-when-non-stakeholders-see-contracts
  *
  * @param disclosure Disclosure, specified in terms of local transaction node IDs
  *  Each node in the transaction will be mapped to a non-empty set of witnesses
  *  for ledger API transactions.
  *  Scenarios unfortunately break this invariant and we can have rollback nodes
  *  at the top with an empty set of witnesses. We still include those
  *  in the map here.
  * @param divulgence
  *     Divulgence, specified in terms of contract IDs.
  *     Note that if this info was produced by blinding a transaction
  *     containing only contract ids, this map may also
  *     contain contracts produced in the same transaction.
  *     We only include contracts that are divulged to a
  *     non-empty set of parties since there is no difference
  *     between not divulging a contract and divulging it to no one.
  */
final case class BlindingInfo(
    disclosure: Relation[NodeId, Party],
    divulgence: Relation[ContractId, Party],
)
