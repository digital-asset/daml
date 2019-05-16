// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.transaction

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.TransactionId
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.AcceptedTransaction
import com.digitalasset.ledger.backend.api.v1.{LedgerBackend, LedgerSyncEvent}
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.server.services.transaction.{OffsetHelper, OffsetSection}

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

protected class TransactionPipeline(ledgerBackend: LedgerBackend) {

  import TransactionPipeline._

  def run(begin: LedgerOffset, end: Option[LedgerOffset]): Source[AcceptedTransaction, NotUsed] =
    Source
      .fromFuture(ledgerBackend.getCurrentLedgerEnd)
      .flatMapConcat { ledgerEnd =>
        OffsetSection(begin, end)(getOffsetHelper(ledgerEnd)) match {
          case Failure(exception) => Source.failed(exception)
          case Success(value) =>
            value match {
              case OffsetSection.Empty => Source.empty
              case OffsetSection.NonEmpty(subscribeFrom, subscribeUntil) =>
                val eventStream = ledgerBackend
                  .ledgerSyncEvents(Some(subscribeFrom))

                subscribeUntil
                  .fold(eventStream)(su => eventStream.untilRequired(su.toLong))
                  .collect { case t: AcceptedTransaction => increaseOffset(t) }
            }
        }
      }

  def getTransactionById(transactionId: TransactionId): Future[Option[AcceptedTransaction]] =
    ledgerBackend
      .getTransactionById(transactionId)
      .map(_.map(increaseOffset))(DEC)

  // the offset we get from LedgerBackend is the actual offset of the entry. We need to return the next one
  // however on the API so clients can resubscribe with the received offset without getting duplicates
  private def increaseOffset(t: AcceptedTransaction) =
    t.copy(offset = Ref.LedgerString.assertFromString((t.offset.toLong + 1).toString))

  private def getOffsetHelper(ledgerEnd: Ref.LedgerString) = {
    new OffsetHelper[Ref.LedgerString] {
      def fromOpaque(opaque: Ref.LedgerString): Try[Ref.LedgerString] = Success(opaque)

      val getLedgerBeginning: Ref.LedgerString = Ref.LedgerString.fromLong(0)

      def getLedgerEnd: Ref.LedgerString = ledgerEnd

      def compare(o1: Ref.LedgerString, o2: Ref.LedgerString): Int =
        java.lang.Long.compare(o1.toLong, o2.toLong)
    }
  }
}

object TransactionPipeline {
  def apply(ledgerBackend: LedgerBackend): TransactionPipeline =
    new TransactionPipeline(ledgerBackend)

  implicit class EventOps(events: Source[LedgerSyncEvent, NotUsed]) {

    /** Consumes the events until the ceiling offset, handling possible gaps as well. */
    def untilRequired(ceilingOffset: Long): Source[LedgerSyncEvent, NotUsed] = {
      events.takeWhile(
        {
          case item =>
            //note that we can have gaps in the increasing offsets!
            (item.offset.toLong + 1) < ceilingOffset //api offsets are +1 compared to backend offsets
        },
        inclusive = true // we need this to be inclusive otherwise the stream will be hanging until a new element from upstream arrives
      )
    }.filter(_.offset.toLong < ceilingOffset) //we need this due to the inclusive nature of the logic above, as in case of gaps a bigger offset might get into the result
  }

}
