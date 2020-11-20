// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Envelope, `Bytes Ordering`}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}

import scala.collection.{SortedMap, breakOut}

final class StateSerializationStrategy(keyStrategy: StateKeySerializationStrategy) {
  def serializeOneState(key: DamlStateKey, value: DamlStateValue): (Key, Value) =
    (keyStrategy.serializeStateKey(key), Envelope.enclose(value))

  def serializeState(state: Map[DamlStateKey, DamlStateValue]): SortedMap[Key, Value] =
    state.map {
      case (key, value) => serializeOneState(key, value)
    }(breakOut)
}
