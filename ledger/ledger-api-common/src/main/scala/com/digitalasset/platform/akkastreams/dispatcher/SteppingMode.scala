package com.digitalasset.platform.akkastreams.dispatcher

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.Future

/** Defines how the progress on the ledger should be mapped to look-up operations  */
sealed abstract class SteppingMode[Index: Ordering, T] extends Product with Serializable {}

object SteppingMode {

  /**
    * Useful when range queries are not possible. For instance streaming a linked-list from Cassandra
    *
    * @param readSuccessor extracts the next index
    * @param readElement   reads the element on the given index
    */
  final case class OneAfterAnother[Index: Ordering, T](
      readSuccessor: (Index, T) => Index,
      readElement: Index => Future[T])
      extends SteppingMode[Index, T]

  /**
    * Applicable when the persistence layer supports efficient range queries.
    *
    * @param range (startInclusive, endExclusive) => Source[(Index, T), NotUsed]
    */
  final case class RangeQuery[Index: Ordering, T](
      range: (Index, Index) => Source[(Index, T), NotUsed])
      extends SteppingMode[Index, T]

}
