// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}

import scala.collection.SortedMap
import scala.collection.compat._

final class StateSerializationStrategy(keyStrategy: StateKeySerializationStrategy) {
  def serializeState(key: DamlStateKey, value: DamlStateValue): Raw.StateEntry =
    (keyStrategy.serializeStateKey(key), Envelope.enclose(value))

  def serializeStateUpdates(
      state: Map[DamlStateKey, DamlStateValue]
  ): SortedMap[Raw.StateKey, Raw.Envelope] =
    implicitly[Factory[Raw.StateEntry, SortedMap[Raw.StateKey, Raw.Envelope]]]
      .fromSpecific(state.view.map { case (key, value) =>
        serializeState(key, value)
      })
}
