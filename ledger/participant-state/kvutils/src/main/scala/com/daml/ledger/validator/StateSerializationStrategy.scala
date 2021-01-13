// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}

import scala.collection.compat._
import scala.collection.SortedMap

final class StateSerializationStrategy(keyStrategy: StateKeySerializationStrategy) {
  def serializeState(key: DamlStateKey, value: DamlStateValue): Raw.KeyValuePair =
    (keyStrategy.serializeStateKey(key), Envelope.enclose(value))

  def serializeStateUpdates(
      state: Map[DamlStateKey, DamlStateValue]
  ): SortedMap[Raw.Key, Raw.Value] =
    implicitly[Factory[Raw.KeyValuePair, SortedMap[Raw.Key, Raw.Value]]]
      .fromSpecific(state.view.map { case (key, value) =>
        serializeState(key, value)
      })
}
