// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value.ContractId

import scala.collection.immutable.TreeSet

/** The inputs of the transaction, that is:
  *   - the contracts were fetched
  *   - all the parties involved
  *   - all the contract keys involved.
  */
/** @param contracts
  *     The contracts fetched by this transaction.
  *
  * @param parties
  *     The contracts created by this transaction.
  *     When the transaction is committed, keys marking the activeness of these
  *     contracts should be created. The key should be a combination of the transaction
  *     id and the relative contract id (that is, the node index).
  * @param keys
  *     The contract keys created or updated as part of the transaction.
  */
case class Inputs(
    contracts: TreeSet[ContractId],
    parties: TreeSet[Ref.Party],
    keys: TreeSet[GlobalKey],
)
