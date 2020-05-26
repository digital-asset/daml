// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{Err, DamlStateMap}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time.Timestamp

import scala.collection.mutable

/** Commit context provides access to state inputs, commit parameters (e.g. record time) and
  * allows committer to set state outputs.
  */
private[kvutils] trait CommitContext {
  def inputs: DamlStateMap
  // NOTE(JM): The outputs must be iterable in deterministic order, hence we
  // keep track of insertion order.
  private val outputOrder: mutable.ArrayBuffer[DamlStateKey] =
    mutable.ArrayBuffer()
  private val outputs: mutable.Map[DamlStateKey, DamlStateValue] =
    mutable.HashMap.empty[DamlStateKey, DamlStateValue]

  def getEntryId: DamlLogEntryId
  def getMaximumRecordTime: Timestamp
  def getRecordTime: Timestamp
  def getParticipantId: ParticipantId

  /** Retrieve value from output state, or if not found, from input state. */
  def get(key: DamlStateKey): Option[DamlStateValue] =
    outputs.get(key).orElse {
      inputs.getOrElse(key, throw Err.MissingInputState(key))
    }

  /** Set a value in the output state. */
  def set(key: DamlStateKey, value: DamlStateValue): Unit = {
    if (!outputs.contains(key)) {
      outputOrder += key
    }
    outputs(key) = value
  }

  /** Modify existing state. Throws if state does not exist. */
  def modify(key: DamlStateKey)(f: DamlStateValue => DamlStateValue): Unit =
    set(key, f(get(key).getOrElse(throw Err.MissingInputState(key))))

  /** Clear the output state. */
  def clear(): Unit = {
    outputOrder.clear()
    outputs.clear()
  }

  /** Get the final output state, in insertion order. */
  def getOutputs: Iterable[(DamlStateKey, DamlStateValue)] =
    outputOrder.map(k => k -> outputs(k))

}
