// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import com.digitalasset.daml.lf.data.Ref.{LedgerString, TransactionIdString}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.types.Ledger

import scala.util.{Failure, Success, Try}

object SandboxEventIdFormatter {

  case class TransactionIdWithIndex(transactionId: TransactionIdString, nodeId: Transaction.NodeId)

  def makeAbsCoid(transactionId: TransactionIdString)(coid: Lf.ContractId): Lf.AbsoluteContractId =
    coid match {
      case a @ Lf.AbsoluteContractId(_) => a
      case Lf.RelativeContractId(txnid) =>
        Lf.AbsoluteContractId(fromTransactionId(transactionId, txnid))
    }
  // this method defines the EventId format used by the sandbox
  def fromTransactionId(transactionId: TransactionIdString, nid: Transaction.NodeId): LedgerString =
    fromTransactionId(transactionId, nid.name)

  private val `#` = LedgerString.assertFromString("#")
  private val `:` = LedgerString.assertFromString(":")

  /** When loading a scenario we get already absolute nids from the ledger -- still prefix them with the transaction
    * id, just to be safe.
    */
  def fromTransactionId(
      transactionId: TransactionIdString,
      nid: Ledger.ScenarioNodeId): LedgerString =
    LedgerString.concat(`#`, transactionId, `:`, nid)

  def split(eventId: String): Option[TransactionIdWithIndex] =
    eventId.split(":") match {
      case Array(transactionId, index) =>
        transactionId.splitAt(1) match {
          case ("#", transIdString) =>
            (for {
              ix <- Try(index.toInt)
              transId <- LedgerString
                .fromString(transIdString)
                .fold(err => Failure(new IllegalArgumentException(err)), Success(_))
              _ <- Try(transId)
            } yield
              TransactionIdWithIndex(transId, Transaction.NodeId.unsafeFromIndex(ix))).toOption
          case _ => None
        }
      case _ => None
    }
}
