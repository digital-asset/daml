// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}

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

  def lookupMaximumLedgerTime(ids: Set[AbsoluteContractId]): Future[Instant]
}
