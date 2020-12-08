// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{DamlStateMap, Err}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable

/** Commit context provides access to state inputs, commit parameters (e.g. record time) and
  * allows committer to set state outputs.
  */
private[kvutils] case class CommitContext(
    private val inputs: DamlStateMap,
    recordTime: Option[Timestamp],
    participantId: ParticipantId,
) {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  // NOTE(JM): The outputs must be iterable in deterministic order, hence we
  // keep track of insertion order.
  private val outputOrder: mutable.ArrayBuffer[DamlStateKey] =
    mutable.ArrayBuffer()
  private val outputs: mutable.Map[DamlStateKey, DamlStateValue] =
    mutable.HashMap.empty[DamlStateKey, DamlStateValue]
  private val accessedInputKeys: mutable.Set[DamlStateKey] = mutable.Set.empty[DamlStateKey]

  var minimumRecordTime: Option[Instant] = None
  var maximumRecordTime: Option[Instant] = None
  var deduplicateUntil: Option[Instant] = None

  // Rejection log entry used for generating an out-of-time-bounds log entry in case of
  // pre-execution.
  var outOfTimeBoundsLogEntry: Option[DamlLogEntry] = None

  def preExecute: Boolean = recordTime.isEmpty

  /** Retrieve value from output state, or if not found, from input state.
    * Throws an exception if the key is not found in either. */
  def get(key: DamlStateKey): Option[DamlStateValue] =
    outputs.get(key).orElse {
      val value = inputs.getOrElse(key, throw Err.MissingInputState(key))
      accessedInputKeys += key
      value
    }

  /** Reads key from input state.
    * Throws an exception if the key is not specified in the input state. */
  def read(key: DamlStateKey): Option[DamlStateValue] = {
    val value = inputs.getOrElse(key, throw Err.MissingInputState(key))
    accessedInputKeys += key
    value
  }

  /** Generates a collection from the inputs as determined by a partial function.
    * Records all keys in the input as being accessed. */
  def collectInputs[B, That](
      partialFunction: PartialFunction[(DamlStateKey, Option[DamlStateValue]), B])(
      implicit bf: CanBuildFrom[Map[DamlStateKey, Option[DamlStateValue]], B, That]): That = {
    val result = inputs.collect(partialFunction)
    inputs.keys.foreach(accessedInputKeys.add)
    result
  }

  /** Set a value in the output state. */
  def set(key: DamlStateKey, value: DamlStateValue): Unit = {
    if (!outputs.contains(key)) {
      outputOrder += key
    }
    outputs(key) = value
  }

  /** Clear the output state. */
  def clear(): Unit = {
    outputOrder.clear()
    outputs.clear()
  }

  /** Get the final output state, in insertion order. */
  def getOutputs: Iterable[(DamlStateKey, DamlStateValue)] =
    outputOrder
      .map(key => key -> outputs(key))
      .filterNot {
        case (key, value) if inputAlreadyContains(key, value) =>
          logger.trace("Identical output found for key {}", key)
          true
        case _ => false
      }

  /** Get the accessed input key set. */
  def getAccessedInputKeys: collection.Set[DamlStateKey] = accessedInputKeys

  private def inputAlreadyContains(key: DamlStateKey, value: DamlStateValue): Boolean =
    inputs.get(key).exists(_.contains(value))
}
