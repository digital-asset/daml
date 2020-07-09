// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{
  DamlStateMap,
  DamlStateMapWithFingerprints,
  Err,
  Fingerprint
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.data.Time.Timestamp
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** Commit context provides access to state inputs, commit parameters (e.g. record time) and
  * allows committer to set state outputs.
  */
private[kvutils] trait CommitContext {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def inputsWithFingerprints: DamlStateMapWithFingerprints
  final def inputs: DamlStateMap = inputsWithFingerprints.map {
    case (key, (value, _)) => (key, value)
  }

  // NOTE(JM): The outputs must be iterable in deterministic order, hence we
  // keep track of insertion order.
  private val outputOrder: mutable.ArrayBuffer[DamlStateKey] =
    mutable.ArrayBuffer()
  private val outputs: mutable.Map[DamlStateKey, DamlStateValue] =
    mutable.HashMap.empty[DamlStateKey, DamlStateValue]
  private val accessedInputKeysAndFingerprints: mutable.Set[(DamlStateKey, Fingerprint)] =
    mutable.Set.empty[(DamlStateKey, Fingerprint)]

  var minimumRecordTime: Option[Instant] = None
  var maximumRecordTime: Option[Instant] = None

  // Rejection log entry used for generating an out-of-time-bounds log entry in case of
  // pre-execution.
  var outOfTimeBoundsLogEntry: Option[DamlLogEntry] = None

  def getRecordTime: Option[Timestamp]
  def getParticipantId: ParticipantId

  def preExecute: Boolean = getRecordTime.isEmpty

  /** Retrieve value from output state, or if not found, from input state. */
  def get(key: DamlStateKey): Option[DamlStateValue] =
    outputs.get(key).orElse {
      val value = inputsWithFingerprints.getOrElse(key, throw Err.MissingInputState(key))
      accessedInputKeysAndFingerprints += key -> value._2
      value._1
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
  def getAccessedInputKeysWithFingerprints: collection.Set[(DamlStateKey, Fingerprint)] =
    accessedInputKeysAndFingerprints

  private def inputAlreadyContains(key: DamlStateKey, value: DamlStateValue): Boolean =
    inputs.get(key).exists(_.contains(value))
}
