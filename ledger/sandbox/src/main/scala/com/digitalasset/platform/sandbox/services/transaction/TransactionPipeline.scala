package com.digitalasset.platform.sandbox.services.transaction

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.ledger.api.domain.LedgerOffset
import com.digitalasset.ledger.backend.api.v1.LedgerSyncEvent.AcceptedTransaction
import com.digitalasset.ledger.backend.api.v1.{LedgerBackend, LedgerSyncEvent}
import com.digitalasset.platform.server.services.transaction.{OffsetHelper, OffsetSection}

import scala.util.{Failure, Success, Try}

protected class TransactionPipeline(ledgerBackend: LedgerBackend) {

  import TransactionPipeline._

  def run(
      requestingParties: List[Party],
      begin: LedgerOffset,
      end: Option[LedgerOffset]): Source[AcceptedTransaction, NotUsed] =
    Source
      .fromFuture(ledgerBackend.getCurrentLedgerEnd)
      .flatMapConcat { ledgerEnd =>
        OffsetSection(begin, end)(getOffsetHelper(ledgerEnd)) match {
          case Failure(exception) => Source.failed(exception)
          case Success(value) =>
            value match {
              case OffsetSection.Empty => Source.empty
              case OffsetSection.NonEmpty(subscribeFrom, subscribeUntil) =>
                ledgerBackend
                  .ledgerSyncEvents(Some(subscribeFrom))
                  .untilRequired(subscribeUntil)
                  .collect {
                    // the offset we get from LedgerBackend is the actual offset of the entry. We need to return the next one
                    // however on the API so clients can resubscribe with the received offset without getting duplicates
                    case t: AcceptedTransaction => t.copy(offset = (t.offset.toLong + 1).toString)
                  }
            }
        }
      }

  private def getOffsetHelper(ledgerEnd: String) = {
    new OffsetHelper[String] {
      override def fromOpaque(opaque: String): Try[String] = Success(opaque)

      override def getLedgerBeginning(): String = "0"

      override def getLedgerEnd(): String = ledgerEnd

      override def compare(o1: String, o2: String): Int =
        java.lang.Long.compare(o1.toLong, o2.toLong)
    }
  }

}

object TransactionPipeline {
  def apply(ledgerBackend: LedgerBackend): TransactionPipeline =
    new TransactionPipeline(ledgerBackend)

  implicit class EventOps(events: Source[LedgerSyncEvent, NotUsed]) {

    /** Consumes the events until the optional ceiling offset */
    def untilRequired(ceilingOffset: Option[String]): Source[LedgerSyncEvent, NotUsed] =
      events.takeWhile(
        {
          case item =>
            //note that we can have gaps in the increasing offsets!
            ceilingOffset.fold(true)(until => until.toLong > (item.offset.toLong + 1))
        },
        inclusive = true
      )
  }

}
