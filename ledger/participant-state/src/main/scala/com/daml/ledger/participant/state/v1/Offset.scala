// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.data.Ref.LedgerString

import scala.util.Try

/** Offsets into streams with hierarchical addressing.
  *
  * We use these [[Offset]]'s to address changes to the participant state.
  * Offsets are arbitrary string values. Offset strings must be strict
  * monotonically increasing according to lexicographical ordering.
  *
  * Ledger implementations are advised to future proof their design
  * of offsets by reserving the first (few) characters to a version
  * indicator, followed by the specific offset scheme for that version.
  * This way it is possible in the future to switch to a different versioning
  * scheme, while making sure that previously created offsets are always
  * less than newer offsets.
  *
  */
final case class Offset private (value: String) extends Ordered[Offset] {

  def toLedgerString: LedgerString = LedgerString.assertFromString(toString)

  override def toString: String = value.toString()

  override def compare(that: Offset): Int = value.compareTo(that.value)
}

object Offset {

  def empty: Offset = Offset("")

  def assertFromString(s: String): Offset =
    Offset(s)

  def fromString(s: String): Try[Offset] = Try(assertFromString(s))

  def fromLong(l: Long, padding: Int = 8): Offset = assertFromString(s"%0${padding}d".format(l))
}
