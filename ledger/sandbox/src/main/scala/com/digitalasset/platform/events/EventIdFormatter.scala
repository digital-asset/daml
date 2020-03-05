// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.events

import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.ledger.EventId

import scala.util.{Failure, Success, Try}

object EventIdFormatter {
  private val `#` = LedgerString.assertFromString("#")
  private val `:` = LedgerString.assertFromString(":")

  case class TransactionIdWithIndex(transactionId: LedgerString, nodeId: Transaction.NodeId)

  def makeAbs(transactionId: LedgerString)(rcoid: Lf.RelativeContractId): LedgerString =
    fromTransactionId(transactionId, rcoid.txnid)

  def makeAbsCoid(transactionId: LedgerString)(coid: Lf.ContractId): Lf.AbsoluteContractId =
    coid match {
      case a @ Lf.AbsoluteContractId(_) => a
      case Lf.RelativeContractId(txnid) =>
        Lf.AbsoluteContractId(fromTransactionId(transactionId, txnid))
    }

  // this method defines the EventId format used by the sandbox
  def fromTransactionId(transactionId: LedgerString, nid: Transaction.NodeId): EventId =
    fromTransactionId(transactionId, LedgerString.fromInt(nid.index))

  /** When loading a scenario we get already absolute nids from the ledger -- still prefix them with the transaction
    * id, just to be safe.
    * `TransactionId`` and `nid`` should be sufficiently small for the concatenation to be smaller than 255 bytes.
    */
  def fromTransactionId(
      transactionId: LedgerString,
      nid: Ledger.ScenarioNodeId,
  ): LedgerString =
    LedgerString.assertConcat(`#`, transactionId, `:`, nid)

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
            } yield TransactionIdWithIndex(transId, Transaction.NodeId(ix))).toOption
          case _ => None
        }
      case _ => None
    }
}
