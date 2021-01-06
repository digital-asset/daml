// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.google.protobuf.ByteString

object Raw {

  case class Key(bytes: ByteString)

  case class Value(bytes: ByteString) {
    def size: Int = bytes.size
  }

  type Pair = (Key, Value)

  implicit val `Key Ordering`: Ordering[Key] = Ordering.by(_.bytes.asReadOnlyByteBuffer)
}
