// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.ledger.participant.state.v1.Update.TransactionAccepted
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert

sealed trait OffsetUpdate extends Product with Serializable {
  def offsetStep: OffsetStep

  def update: Update
}

object OffsetUpdate {
  def unapply(offsetUpdate: OffsetUpdate): Option[(OffsetStep, Update)] =
    Some((offsetUpdate.offsetStep, offsetUpdate.update))

  final case class PreparedTransactionInsert(
      offsetStep: OffsetStep,
      update: TransactionAccepted,
      preparedInsert: PreparedInsert)
      extends OffsetUpdate

  final case class OffsetStepUpdatePair(offsetStep: OffsetStep, update: Update) extends OffsetUpdate
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
