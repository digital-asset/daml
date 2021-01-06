// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.google.protobuf.ByteString

sealed trait Raw {
  def bytes: ByteString

  final def size: Long = bytes.size.toLong
}

object Raw {

  final case class Key(override val bytes: ByteString) extends Raw

  final case class Value(override val bytes: ByteString) extends Raw

  type Pair = (Key, Value)

  implicit val `Key Ordering`: Ordering[Key] = Ordering.by(_.bytes.asReadOnlyByteBuffer)

}
