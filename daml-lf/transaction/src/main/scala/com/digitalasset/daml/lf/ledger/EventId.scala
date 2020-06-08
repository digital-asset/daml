// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package ledger

import com.daml.lf.data.Ref._
import com.daml.lf.transaction.{Transaction => Tx}

import scala.util.Try

// transactionId should be small so the concatenation in toLedgerString does not exceed 255 chars
case class EventId(
    transactionId: LedgerString,
    nodeId: Tx.NodeId
) {
  lazy val toLedgerString: LedgerString = {
    val builder = StringBuilder.newBuilder
    builder += '#'
    builder ++= transactionId
    builder += ':'
    builder ++= nodeId.index.toString
    LedgerString.assertFromString(builder.result())
  }
}

object EventId {
  def fromString(s: String): Either[String, EventId] = {
    def err = Left(s"""cannot parse eventId $s""")

    s.split(":") match {
      case Array(transactionId, index) =>
        transactionId.splitAt(1) match {
          case ("#", transIdString) =>
            for {
              ix <- Try(index.toInt).fold(_ => err, Right(_))
              transId <- LedgerString.fromString(transIdString)
            } yield EventId(transId, Tx.NodeId(ix))
          case _ => err
        }
      case _ => err
    }
  }

  def assertFromString(s: String): EventId =
    data.assertRight(fromString(s))
}
