// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import LedgerOffset.{LedgerBoundary, Value}
import Value.Boundary
import com.digitalasset.canton.fetchcontracts.domain
import scalaz.Liskov.<~<
import scalaz.Order
import scalaz.syntax.order.*
import spray.json.{JsNull, JsonWriter}

 sealed abstract class BeginBookmark[+Off] extends Product with Serializable {
  def toLedgerApi(implicit ev: Off <~< domain.Offset): LedgerOffset =
    this match {
      case AbsoluteBookmark(offset) => domain.Offset.toLedgerApi(ev(offset))
      case LedgerBegin => LedgerOffset(Boundary(LedgerBoundary.LEDGER_BEGIN))
    }

  def map[B](f: Off => B): BeginBookmark[B] = this match {
    case AbsoluteBookmark(offset) => AbsoluteBookmark(f(offset))
    case LedgerBegin => LedgerBegin
  }

  def toOption: Option[Off] = this match {
    case AbsoluteBookmark(offset) => Some(offset)
    case LedgerBegin => None
  }
}
 final case class AbsoluteBookmark[+Off](offset: Off) extends BeginBookmark[Off]
 case object LedgerBegin extends BeginBookmark[Nothing]

 object BeginBookmark {
  implicit def jsonWriter[Off: JsonWriter]: JsonWriter[BeginBookmark[Off]] =
    (obj: BeginBookmark[Off]) => {
      val ev = implicitly[JsonWriter[Off]]
      obj match {
        case AbsoluteBookmark(offset) => ev.write(offset)
        case LedgerBegin => JsNull
      }
    }

  import scalaz.Ordering.{EQ, LT, GT}

  implicit def `BeginBookmark order`[Off: Order]: Order[BeginBookmark[Off]] = {
    case (AbsoluteBookmark(a), AbsoluteBookmark(b)) => a ?|? b
    case (LedgerBegin, LedgerBegin) => EQ
    case (LedgerBegin, AbsoluteBookmark(_)) => LT
    case (AbsoluteBookmark(_), LedgerBegin) => GT
  }
}
