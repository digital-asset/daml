// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

sealed trait PersistentEventType extends Product with Serializable {
  def asInt: Int
}

// WARNING! The PersistentEventType mappings are stored in DB, changing them only allowed in a backwards compatible way to ensure data continuity!
// Changing these should be reflected in the debug-views (see V2_1__lapi_3.0_views.sql)
object PersistentEventType {
  // activations
  sealed abstract class ActivationPersistentEventType(override val asInt: Int)
      extends PersistentEventType
  case object Create extends ActivationPersistentEventType(1)
  case object Assign extends ActivationPersistentEventType(2)
  // deactivations
  sealed abstract class DeactivationPersistentEventType(override val asInt: Int)
      extends PersistentEventType
  case object ConsumingExercise extends DeactivationPersistentEventType(3)
  case object Unassign extends DeactivationPersistentEventType(4)
  // various witnessed
  sealed abstract class VariousWitnessedPersistentEventType(override val asInt: Int)
      extends PersistentEventType
  case object NonConsumingExercise extends VariousWitnessedPersistentEventType(5)
  case object WitnessedCreate extends VariousWitnessedPersistentEventType(6)
  case object WitnessedConsumingExercise extends VariousWitnessedPersistentEventType(7)
  // topology transactions
  sealed abstract class TopologyTransactionPersistentEventType(override val asInt: Int)
      extends PersistentEventType
  case object PartyToParticipant extends TopologyTransactionPersistentEventType(8)

  val allEventTypes: Seq[PersistentEventType] = List(
    Create,
    Assign,
    ConsumingExercise,
    Unassign,
    NonConsumingExercise,
    WitnessedCreate,
    WitnessedConsumingExercise,
    PartyToParticipant,
  )

  private val formIntMap: Map[Int, PersistentEventType] =
    allEventTypes
      .map(persistentEventType => persistentEventType.asInt -> persistentEventType)
      .toMap

  def fromInt(i: Int): PersistentEventType =
    formIntMap.getOrElse(
      i,
      throw new IllegalStateException(
        s"Invalid Int $i - no such PersistentEventType can be found."
      ),
    )

  assert(allEventTypes.sizeIs == 8)
  assert(allEventTypes.toSet.sizeIs == 8)
  assert(formIntMap.sizeIs == 8)
}
