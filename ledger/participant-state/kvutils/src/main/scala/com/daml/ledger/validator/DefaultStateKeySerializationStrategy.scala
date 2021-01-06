// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Raw

/**
  * Default state key serialization strategy that does not prefix keys.
  */
object DefaultStateKeySerializationStrategy extends StateKeySerializationStrategy {
  override def serializeStateKey(key: DamlStateKey): Raw.Key =
    Raw.Key(key.toByteString)

  override def deserializeStateKey(input: Raw.Key): DamlStateKey =
    DamlStateKey.parseFrom(input.bytes)
}
