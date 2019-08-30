// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.index.v2.ContractStore
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Transaction.{Value => TxValue}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState

import scala.concurrent.{ExecutionContext, Future}

class SandboxContractStore(ledger: ReadOnlyLedger) extends ContractStore {
  private[this] def canSeeContract(submitter: Party, c: ActiveLedgerState.Contract): Boolean =
    c match {
      case ac: ActiveLedgerState.ActiveContract =>
        // ^ only parties disclosed or divulged to can lookup; see https://github.com/digital-asset/daml/issues/10
        // and https://github.com/digital-asset/daml/issues/751 .
        Party fromString submitter exists (p => ac.witnesses(p) || ac.divulgences.contains(p))
      case dc: ActiveLedgerState.DivulgedContract =>
        // ^ only parties disclosed or divulged to can lookup; see https://github.com/digital-asset/daml/issues/10
        // and https://github.com/digital-asset/daml/issues/751 .
        Party fromString submitter exists (p => dc.divulgences.contains(p))
    }

  override def lookupActiveContract(submitter: Party, contractId: Value.AbsoluteContractId)
    : Future[Option[Value.ContractInst[TxValue[Value.AbsoluteContractId]]]] =
    ledger
      .lookupContract(contractId)
      .map(_.collect {
        case ac if canSeeContract(submitter, ac) => ac.contract
      })(DEC)

  override def lookupContractKey(
      submitter: Party,
      key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] = {
    implicit val ec: ExecutionContext = DEC
    ledger.lookupKey(key).flatMap {
      // note that we need to check visibility for keys, too, otherwise we leak the existence of a non-divulged
      // contract if we return `Some`.
      case None => Future.successful(None)
      case Some(cid) =>
        ledger.lookupContract(cid) map {
          _ flatMap (ac => if (canSeeContract(submitter, ac)) Some(cid) else None)
        }
    }
  }
}
