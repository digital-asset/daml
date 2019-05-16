// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.data.Ref

/** Offsets into streams with hierarchical addressing.
  *
  * We use these [[Offset]]'s to address changes to the participant state.
  * We allow for array of [[Int]] to allow for hierarchical addresses.
  * These [[Int]] values are expected to be positive. Offsets are ordered by
  * lexicographic ordering of the array elements.
  *
  * A typical use case for [[Offset]]s would be addressing a transaction in a
  * blockchain by `[<blockheight>, <transactionId>]`. Depending on the
  * structure of the underlying ledger these offsets are more or less
  * nested, which is why we use an array of [[Int]]s. The expectation is
  * though that there usually are few elements in the array.
  *
  */
final case class Offset(private val xs: Array[Long]) extends Ordered[Offset] {
  def toTransactionId: Ref.TransactionId =
    // It is safe to concatenate number and "-" to obtain a valid transactionId
    Ref.LedgerString.assertFromString(components.mkString("-"))

  def components: Iterable[Long] = xs

  override def equals(that: Any): Boolean = that match {
    case o: Offset => this.compare(o) == 0
    case _ => false
  }

  def compare(that: Offset): Int =
    scala.math.Ordering.Iterable[Long].compare(this.xs.toIterable, that.xs.toIterable)
}

object Offset {

  /** Create an offset from a string of form 1-2-3. Throws
    * NumberFormatException on misformatted strings.
    */
  def assertFromString(s: String): Offset =
    Offset(s.split('-').map(_.toLong))

}
