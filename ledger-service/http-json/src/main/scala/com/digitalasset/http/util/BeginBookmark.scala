// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import LedgerOffset.{LedgerBoundary, Value}
import Value.Boundary

import scalaz.Liskov.<~<

private[http] sealed abstract class BeginBookmark[+Off] extends Product with Serializable {
  def toLedgerApi(implicit ev: Off <~< domain.Offset): LedgerOffset =
    this match {
      case AbsoluteBookmark(offset) => domain.Offset.toLedgerApi(ev(offset))
      case LedgerBegin => LedgerOffset(Boundary(LedgerBoundary.LEDGER_BEGIN))
    }

  def toOption: Option[Off] = this match {
    case AbsoluteBookmark(offset) => Some(offset)
    case LedgerBegin => None
  }
}
private[http] final case class AbsoluteBookmark[+Off](offset: Off) extends BeginBookmark[Off]
private[http] case object LedgerBegin extends BeginBookmark[Nothing]
