// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope, `Bytes Ordering`}
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.google.protobuf.ByteString

import scala.collection.{SortedMap, breakOut}

/**
  * Determines how we namespace and serialize state keys.
  */
trait StateKeySerializationStrategy {
  def serializeStateKey(key: DamlStateKey): ByteString

  def deserializeStateKey(input: Bytes): DamlStateKey

  def serializeState(state: Map[DamlStateKey, DamlStateValue]): SortedMap[Key, Value] =
    state.map {
      case (key, value) => (serializeStateKey(key), Envelope.enclose(value))
    }(breakOut)
}

object StateKeySerializationStrategy {
  def createDefault(): StateKeySerializationStrategy = DefaultStateKeySerializationStrategy
}
