// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import java.time.Instant

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}

import scala.concurrent.Future

/**
  * Meant be used for optimistic contract lookups before command submission.
  */
trait ContractStore {
  def lookupActiveContract(
      submitter: Ref.Party,
      contractId: AbsoluteContractId
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def lookupContractKey(submitter: Party, key: GlobalKey): Future[Option[AbsoluteContractId]]

  /**
    * @return The maximum ledger effective time of all contracts in ids, fails as follows:
    *         - if ids is empty or not all the non-divulged ids can be found, a failed [[Future]]
    *         - if all ids are found but each refer to a divulged contract, a successful [[None]]
    */
  def lookupMaximumLedgerTime(ids: Set[AbsoluteContractId]): Future[Option[Instant]]
}
