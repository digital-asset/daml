// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.transaction

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId

/** This gives implicit and explicit divulgence info. Explicit
  * disclosure (also known as simply "disclosure") tells us which
  * nodes to communicate to which parties, while implicit disclosure
  * (also known as "divulgence") tells us what to communicate to
  * each participant node so that they can perform post-commit
  * validation. Note that implicit disclosure can also divulge
  * absolute contract ids -- e.g. contract ids that were created
  * _outside_ this transaction.
  */
case class BlindingInfo(
    /** Also simply known as "disclosure" */
    explicitDisclosure: Relation[Transaction.NodeId, Party],
    /** Also known as "divulgence" */
    localImplicitDisclosure: Relation[Transaction.NodeId, Party],
    globalImplicitDisclosure: Relation[AbsoluteContractId, Party]) {
  def localDisclosure: Relation[Transaction.NodeId, Party] =
    Relation.union(explicitDisclosure, localImplicitDisclosure)
}
