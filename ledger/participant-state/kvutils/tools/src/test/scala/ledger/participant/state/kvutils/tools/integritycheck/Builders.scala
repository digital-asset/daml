// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.`export`.WriteSet
import com.google.protobuf.ByteString

object Builders {
  def bytes(text: String): ByteString =
    ByteString.copyFromUtf8(text)

  def writeSet(values: (String, String)*): WriteSet =
    values.map {
      case (key, value) => bytes(key) -> bytes(value)
    }
}
