// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.participant.state.kvutils.Raw

/** Determines how we namespace and serialize state keys.
  */
trait StateKeySerializationStrategy {
  def serializeStateKey(key: DamlStateKey): Raw.Key

  def deserializeStateKey(input: Raw.Key): DamlStateKey
}

object StateKeySerializationStrategy {
  def createDefault(): StateKeySerializationStrategy = DefaultStateKeySerializationStrategy
}
