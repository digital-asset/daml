// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.store.DamlStateKey

/** Default state key serialization strategy that does not prefix keys.
  */
object DefaultStateKeySerializationStrategy extends StateKeySerializationStrategy {
  override def serializeStateKey(key: DamlStateKey): Raw.StateKey =
    Raw.StateKey(key)

  override def deserializeStateKey(input: Raw.StateKey): DamlStateKey =
    DamlStateKey.parseFrom(input.bytes)
}
