// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import com.digitalasset.daml.lf.data.Ref.{LedgerString, TransactionIdString}
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.value.Value.RelativeContractId
import com.digitalasset.daml.lf.value.{Value => Lf}

import scala.util.{Failure, Success, Try}

object EventIdFormatter {
  private val `#` = LedgerString.assertFromString("#")
  private val `:` = LedgerString.assertFromString(":")

  case class TransactionIdWithIndex(transactionId: TransactionIdString, nodeId: Transaction.NodeId)

  def makeAbsCoid(transactionId: TransactionIdString)(coid: Lf.ContractId): Lf.AbsoluteContractId =
    coid match {
      case a @ Lf.AbsoluteContractId(_) => a
      case rcoid: Lf.RelativeContractId =>
        Lf.AbsoluteContractId(fromTransactionId(transactionId, rcoid))
    }

  // this method defines the EventId format used by the sandbox
  def fromTransactionId(transactionId: TransactionIdString, nid: Transaction.NodeId): LedgerString =
    fromTransactionId(transactionId, nid.name)

  // this method defines the EventId format used by the sandbox
  def fromTransactionId(
      transactionId: TransactionIdString,
      rcoid: RelativeContractId
  ): LedgerString =
    fromTransactionId(transactionId, rcoid.name)

  /** When loading a scenario we get already absolute nids from the ledger -- still prefix them with the transaction
    * id, just to be safe.
    */
  private def fromTransactionId(
      transactionId: TransactionIdString,
      nid: Ledger.ScenarioNodeId,
  ): LedgerString =
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
