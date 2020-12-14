// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.ledger.participant.state.v1.Update.TransactionAccepted
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.metrics.TelemetryContext

case class OffsetUpdateWithContext(offsetUpdate: OffsetUpdate)(implicit val telemetryContext: TelemetryContext)

sealed trait OffsetUpdate {
  def offset: Offset

  def update: Update
}

object OffsetUpdate {
  def unapply(offsetUpdate: OffsetUpdate): Option[(Offset, Update)] =
    Some((offsetUpdate.offset, offsetUpdate.update))

  final case class PreparedTransactionInsert(
      offset: Offset,
      update: TransactionAccepted,
      preparedInsert: PreparedInsert)
      extends OffsetUpdate

  final case class OffsetUpdatePair(offset: Offset, update: Update) extends OffsetUpdate
}
