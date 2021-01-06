// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.`export`.WriteSet
import com.daml.ledger.validator.Raw
import com.google.protobuf.ByteString

object Builders {

  def writeSet(values: (String, String)*): WriteSet =
    values.map {
      case (key, value) =>
        Raw.Key(ByteString.copyFromUtf8(key)) -> Raw.Value(ByteString.copyFromUtf8(value))
    }
}
