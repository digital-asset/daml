// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.participant.state.v1.{MetadataUpdate, Offset, Update}
import com.daml.ledger.participant.state.v1.Update.TransactionAccepted
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert

sealed trait OffsetUpdate[+T <: Update] extends Product with Serializable {
  def offsetStep: OffsetStep

  def update: T
}

object OffsetUpdate {
  def unapply[T <: Update](offsetUpdate: OffsetUpdate[T]): Option[(OffsetStep, T)] =
    Some((offsetUpdate.offsetStep, offsetUpdate.update))

  sealed trait PreparedUpdate extends OffsetUpdate[Update]

  final case class OffsetStepUpdatePair[+T <: Update](offsetStep: OffsetStep, update: T)
      extends OffsetUpdate[T]

  final case class PreparedTransactionInsert(
      offsetStep: OffsetStep,
      update: TransactionAccepted,
      preparedInsert: PreparedInsert,
  ) extends PreparedUpdate

  final case class MetadataUpdateStep(offsetStep: OffsetStep, update: MetadataUpdate)
      extends PreparedUpdate
}

sealed trait OffsetStep extends Product with Serializable {
  def offset: Offset
}

object OffsetStep {
  def apply(previousOffset: Option[Offset], offset: Offset): OffsetStep = previousOffset match {
    case Some(prevOffset) => IncrementalOffsetStep(prevOffset, offset)
    case None => CurrentOffset(offset)
  }
}

final case class CurrentOffset(offset: Offset) extends OffsetStep
final case class IncrementalOffsetStep(previousOffset: Offset, offset: Offset) extends OffsetStep
