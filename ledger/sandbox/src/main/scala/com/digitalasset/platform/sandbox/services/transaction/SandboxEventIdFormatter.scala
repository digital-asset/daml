// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import com.digitalasset.daml.lf.data.Ref.{LedgerName, TransactionId}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.types.LedgerForScenarios

import scala.util.Try

object SandboxEventIdFormatter {

  case class TransactionIdWithIndex(transactionId: TransactionId, nodeId: Transaction.NodeId)

  def makeAbsCoid(transactionId: TransactionId)(coid: Lf.VContractId): Lf.AbsoluteContractId =
    coid match {
      case a @ Lf.AbsoluteContractId(_) => a
      case Lf.RelativeContractId(txnid) =>
        Lf.AbsoluteContractId(fromTransactionId(transactionId, txnid))
    }
  // this method defines the EventId format used by the sandbox
  def fromTransactionId(transactionId: TransactionId, nid: Transaction.NodeId): LedgerName =
    fromTransactionId(transactionId, nid.name)

  private val `#` = LedgerName.assertFromString("#")
  private val `:` = LedgerName.assertFromString(":")

  /** When loading a scenario we get already absolute nids from the ledger -- still prefix them with the transaction
    * id, just to be safe.
    */
  def fromTransactionId(
      transactionId: TransactionId,
      nid: LedgerForScenarios.ScenarioNodeId): LedgerName =
    LedgerName.concat(`#`, transactionId, `:`, nid)

  def split(eventId: String): Option[TransactionIdWithIndex] =
    eventId.split(":") match {
      case Array(transactionId, index) =>
        transactionId.splitAt(1) match {
          case ("#", transId) =>
            (for {
              ix <- Try(index.toInt)
              _ <- Try(transId.toLong)
            } yield
              TransactionIdWithIndex(
                LedgerName.assertFromString(transId),
                Transaction.NodeId.unsafeFromIndex(ix))).toOption
          case _ => None
        }
      case _ => None
    }
}
