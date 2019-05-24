// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.services.transaction

import java.util.Comparator

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.platform.server.api.validation.ErrorFactories

import scala.util.{Failure, Success, Try}

sealed abstract class OffsetSection[+T] extends Product with Serializable

trait OffsetHelper[Offset] extends Comparator[Offset] {
  def fromOpaque(opaque: Ref.LedgerString): Try[Offset]

  def getLedgerBeginning: Offset

  def getLedgerEnd: Offset
}

object OffsetSection extends ErrorFactories {

  case object Empty extends OffsetSection[Nothing]

  final case class NonEmpty[Offset](begin: Offset, end: Option[Offset])
      extends OffsetSection[Offset]

  def apply[Offset](begin: LedgerOffset, end: Option[LedgerOffset])(
      implicit oh: OffsetHelper[Offset]): Try[OffsetSection[Offset]] = {

    lazy val ledgerEnd = oh.getLedgerEnd

    val parsedBegin = begin match {
      case LedgerOffset.Absolute(opaqueCursor) =>
        oh.fromOpaque(opaqueCursor)
      case LedgerOffset.LedgerBegin =>
        Success(oh.getLedgerBeginning)
      case LedgerOffset.LedgerEnd =>
        Success(ledgerEnd)
    }

    end.fold[Try[OffsetSection[Offset]]](parsedBegin.map(NonEmpty(_, None))) {
      case LedgerOffset.Absolute(opaqueCursor) =>
        parsedBegin.flatMap { begin =>
          oh.fromOpaque(opaqueCursor).flatMap(end => checkOrdering(begin, end))
        }
      case LedgerOffset.LedgerBegin =>
        parsedBegin.flatMap(begin => checkOrdering[Offset](begin, oh.getLedgerBeginning))
      case LedgerOffset.LedgerEnd =>
        parsedBegin.flatMap(checkOrdering(_, ledgerEnd))
    }

  }

  private def checkOrdering[Offset](begin: Offset, end: Offset)(
      implicit oh: OffsetHelper[Offset]): Try[OffsetSection[Offset]] = {
    val comparisonResult = oh.compare(begin, end)
    if (comparisonResult == 0) Success(OffsetSection.Empty)
    else if (comparisonResult > 0)
      Failure(invalidArgument(s"End offset $end is before Begin offset $begin."))
    else Success(NonEmpty(begin, Some(end)))
  }
}
