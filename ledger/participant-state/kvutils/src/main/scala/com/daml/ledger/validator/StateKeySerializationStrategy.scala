// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.Bytes
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.google.protobuf.ByteString

/**
  * Determines how we namespace and serialize state keys.
  */
trait StateKeySerializationStrategy {
  def serializeStateKey(key: DamlStateKey): ByteString

  def deserializeStateKey(input: Bytes): DamlStateKey
}

object StateKeySerializationStrategy {
  def createDefault(): StateKeySerializationStrategy = DefaultStateKeySerializationStrategy
}
