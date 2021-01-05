// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey

/**
  * Default state key serialization strategy that does not prefix keys.
  */
object DefaultStateKeySerializationStrategy extends StateKeySerializationStrategy {
  override def serializeStateKey(key: DamlStateKey): Bytes = key.toByteString

  override def deserializeStateKey(input: Bytes): DamlStateKey = DamlStateKey.parseFrom(input)
}
