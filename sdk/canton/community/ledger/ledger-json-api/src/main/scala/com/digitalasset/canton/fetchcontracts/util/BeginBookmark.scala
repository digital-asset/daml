// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.fetchcontracts.util

import com.digitalasset.canton.fetchcontracts.Offset
import scalaz.Liskov.<~<
import scalaz.Order
import scalaz.syntax.order.*
import spray.json.{JsNull, JsonWriter}

sealed abstract class BeginBookmark[+Off] extends Product with Serializable {
  def toLedgerApi(implicit ev: Off <~< Offset): String =
    this match {
      case AbsoluteBookmark(offset) => Offset.unwrap(ev(offset))
      case ParticipantBegin =>
        ""
    }

  def map[B](f: Off => B): BeginBookmark[B] = this match {
    case AbsoluteBookmark(offset) => AbsoluteBookmark(f(offset))
    case ParticipantBegin => ParticipantBegin
  }

  def toOption: Option[Off] = this match {
    case AbsoluteBookmark(offset) => Some(offset)
    case ParticipantBegin => None
  }
}
final case class AbsoluteBookmark[+Off](offset: Off) extends BeginBookmark[Off]
case object ParticipantBegin extends BeginBookmark[Nothing]

object BeginBookmark {
  implicit def jsonWriter[Off: JsonWriter]: JsonWriter[BeginBookmark[Off]] =
    (obj: BeginBookmark[Off]) => {
      val ev = implicitly[JsonWriter[Off]]
      obj match {
        case AbsoluteBookmark(offset) => ev.write(offset)
        case ParticipantBegin => JsNull
      }
    }

  import scalaz.Ordering.{EQ, LT, GT}

  implicit def `BeginBookmark order`[Off: Order]: Order[BeginBookmark[Off]] = {
    case (AbsoluteBookmark(a), AbsoluteBookmark(b)) => a ?|? b
    case (ParticipantBegin, ParticipantBegin) => EQ
    case (ParticipantBegin, AbsoluteBookmark(_)) => LT
    case (AbsoluteBookmark(_), ParticipantBegin) => GT
  }
}
