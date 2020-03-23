// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.data.Bytes
import scalaz.Tag

/** Offsets into streams with hierarchical addressing.
  *
  * We use these [[Offset]]'s to address changes to the participant state.
  * Offsets are opaque values that must be must be strictly
  * increasing according to lexicographical ordering.
  *
  * Ledger implementations are advised to future proof their design
  * of offsets by reserving the first (few) bytes for a version
  * indicator, followed by the specific offset scheme for that version.
  * This way it is possible in the future to switch to a different versioning
  * scheme, while making sure that previously created offsets are always
  * less than newer offsets.
  *
  */
sealed trait OffsetTag

object Offset {

  val tag = Tag.of[OffsetTag]

  def apply(s: Bytes): Offset = tag(s)

  def unwrap(x: Offset): Bytes = tag.unwrap(x)

  val begin: Offset = Offset(Bytes.fromByteArray(Array(0: Byte)))

  implicit val `Offset Ordering`: Ordering[Offset] = Ordering.by(unwrap)

}
