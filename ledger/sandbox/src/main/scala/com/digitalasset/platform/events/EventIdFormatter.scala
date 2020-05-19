// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.events

import com.daml.lf.data.Ref.LedgerString
import com.daml.lf.transaction.Transaction
import com.daml.lf.types.Ledger
import com.daml.ledger.EventId

import scala.util.{Failure, Success, Try}

object EventIdFormatter {
  private val `#` = LedgerString.assertFromString("#")
  private val `:` = LedgerString.assertFromString(":")

  case class TransactionIdWithIndex(transactionId: LedgerString, nodeId: Transaction.NodeId)

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
