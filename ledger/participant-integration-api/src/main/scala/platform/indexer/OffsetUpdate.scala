// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert

sealed trait OffsetUpdate extends Product with Serializable {
  def offsetStep: OffsetStep
  def update: state.Update
}

object OffsetUpdate {
  def unapply(offsetUpdate: OffsetUpdate): Some[(OffsetStep, state.Update)] =
    Some((offsetUpdate.offsetStep, offsetUpdate.update))

  def apply(offsetStep: OffsetStep, update: state.Update): OffsetUpdate =
    OffsetUpdateImpl(offsetStep, update)

  final case class PreparedTransactionInsert(
      offsetStep: OffsetStep,
      update: state.Update.TransactionAccepted,
      preparedInsert: PreparedInsert,
  ) extends OffsetUpdate

  private final case class OffsetUpdateImpl(offsetStep: OffsetStep, update: state.Update)
      extends OffsetUpdate
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
