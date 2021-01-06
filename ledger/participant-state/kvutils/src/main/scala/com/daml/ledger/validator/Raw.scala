// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.google.protobuf.ByteString

trait Raw {
  def bytes: ByteString

  final def size: Int = bytes.size
}

object Raw {

  case class Key(override val bytes: ByteString) extends Raw

  case class Value(override val bytes: ByteString) extends Raw

  type Pair = (Key, Value)

  implicit val `Key Ordering`: Ordering[Key] = Ordering.by(_.bytes.asReadOnlyByteBuffer)

}
