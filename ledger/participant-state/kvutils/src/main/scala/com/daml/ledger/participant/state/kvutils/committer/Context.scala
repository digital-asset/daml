package com.daml.ledger.participant.state.kvutils.committer

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.data.Time.Timestamp

import scala.collection.mutable

trait Context {
  def inputs: DamlStateMap
  // NOTE(JM): The outputs must be iterable in deterministic order, hence we
  // keep track of insertion order.
  private val outputOrder: mutable.ArrayBuffer[DamlStateKey] =
    mutable.ArrayBuffer()

  private val outputs: mutable.Map[DamlStateKey, DamlStateValue] =
    mutable.HashMap.empty[DamlStateKey, DamlStateValue]

  def getRecordTime: Timestamp
  def getParticipantId: ParticipantId

  def getOutputs: Iterable[(DamlStateKey, DamlStateValue)] =
    outputOrder.map(k => k -> outputs(k))

  /** Finish the commit, skipping the remaining steps. */
  def done[Void](logEntry: DamlLogEntry): Void

  /** Retrieve value from state. */
  def get(key: DamlStateKey): Option[DamlStateValue] =
    outputs.get(key).orElse(inputs.get(key))

  /** Set a value in state. */
  def set(key: DamlStateKey, value: DamlStateValue): Unit = {
    outputOrder += key
    outputs(key) = value
  }

}
