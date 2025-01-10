// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ShowUtil.*

object Pruning {

  trait LedgerPruningError extends Product with Serializable { def message: String }

  case object LedgerPruningCancelledDueToShutdown extends LedgerPruningError {
    override def message: String = "Cancelled due to shutdown"
  }

  case object LedgerPruningNothingToPrune extends LedgerPruningError {
    val message = "Nothing to prune"
  }

  final case class LedgerPruningInternalError(message: String) extends LedgerPruningError

  final case class LedgerPruningOffsetUnsafeDomain(synchronizerId: SynchronizerId)
      extends LedgerPruningError {
    override def message =
      s"No safe-to-prune offset for synchronizer $synchronizerId."
  }

  case object LedgerPruningOffsetAfterLedgerEnd extends LedgerPruningError {
    override def message =
      s"Provided pruning offset is ahead of ledger-end."
  }

  final case class LedgerPruningOffsetUnsafeToPrune(
      offset: Offset,
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      cause: String,
      lastSafeOffset: Option[Offset],
  ) extends LedgerPruningError {
    override def message =
      show"Unsafe to prune offset $offset due to the event for $synchronizerId with record time $recordTime"
  }

  final case class LedgerPruningNotPossibleDuringHardMigration(
      synchronizerId: SynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  ) extends LedgerPruningError {
    override def message =
      s"The synchronizer $synchronizerId can not be pruned as there is a pending synchronizer migration: $status"
  }

  final case class PurgingUnknownDomain(synchronizerId: SynchronizerId) extends LedgerPruningError {
    override def message = s"Domain $synchronizerId does not exist."
  }

  final case class PurgingOnlyAllowedOnInactiveDomain(
      synchronizerId: SynchronizerId,
      status: SynchronizerConnectionConfigStore.Status,
  ) extends LedgerPruningError {
    override def message: String =
      s"Domain $synchronizerId status needs to be inactive, but is ${status.getClass.getSimpleName}"
  }
}
