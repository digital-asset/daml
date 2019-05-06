package com.digitalasset.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.platform.common.util.DirectExecutionContext

import scala.concurrent.Future

/** Defines how the progress on the ledger should be mapped to look-up operations */
sealed abstract class SubSource[Index: Ordering, T]
    extends ((Index, Index) => Source[(Index, T), NotUsed]) {

  /** Returns a Source emitting items for the given range */
  def subSource(startInclusive: Index, endExclusive: Index): Source[(Index, T), NotUsed]

  override def apply(startInclusive: Index, endExclusive: Index): Source[(Index, T), NotUsed] =
    subSource(startInclusive, endExclusive)
}

object SubSource {

  /**
    * Useful when range queries are not possible. For instance streaming a linked-list from Cassandra
    *
    * @param readSuccessor extracts the next index
    * @param readElement   reads the element on the given index
    */
  final case class OneAfterAnother[Index: Ordering, T](
      readSuccessor: (Index, T) => Index,
      readElement: Index => Future[T])
      extends SubSource[Index, T] {
    override def subSource(
        startInclusive: Index,
        endExclusive: Index): Source[(Index, T), NotUsed] =
      Source
        .unfoldAsync[Index, (Index, T)](startInclusive) { i =>
          if (i == endExclusive) Future.successful(None)
          else
            readElement(i).map { t =>
              val nextIndex = readSuccessor(i, t)
              Some((nextIndex, (i, t)))
            }(DirectExecutionContext)
        }

  }

  /**
    * Applicable when the persistence layer supports efficient range queries.
    *
    * @param getRange (startInclusive, endExclusive) => Source[(Index, T), NotUsed]
    */
  final case class RangeSource[Index: Ordering, T](
      getRange: (Index, Index) => Source[(Index, T), NotUsed])
      extends SubSource[Index, T] {
    override def subSource(
        startInclusive: Index,
        endExclusive: Index): Source[(Index, T), NotUsed] = getRange(startInclusive, endExclusive)
  }

}
