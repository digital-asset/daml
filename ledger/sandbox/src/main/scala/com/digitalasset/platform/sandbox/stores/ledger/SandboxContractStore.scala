// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.index.v2.ContractStore
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.dec.{DirectExecutionContext => DEC}

import scala.concurrent.{ExecutionContext, Future}

class SandboxContractStore(ledger: ReadOnlyLedger) extends ContractStore {
  override def lookupActiveContract(submitter: Party, contractId: Value.AbsoluteContractId)
    : Future[Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]] =
    ledger
      .lookupContract(contractId, submitter)
      .map(_.map(_.contract))(DEC)

  override def lookupContractKey(
      submitter: Party,
      key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] = {
    implicit val ec: ExecutionContext = DEC
    ledger.lookupKey(key, submitter)
  }
}
