// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp}
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.topology.DomainId
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

  final case class LedgerPruningOffsetUnsafeDomain(domain: DomainId) extends LedgerPruningError {
    override def message =
      s"No safe-to-prune offset for domain $domain."
  }

  case object LedgerPruningOffsetAfterLedgerEnd extends LedgerPruningError {
    override def message =
      s"Provided pruning offset is ahead of ledger-end."
  }

  final case class LedgerPruningOffsetUnsafeToPrune(
      offset: AbsoluteOffset,
      domainId: DomainId,
      recordTime: CantonTimestamp,
      cause: String,
      lastSafeOffset: Option[AbsoluteOffset],
  ) extends LedgerPruningError {
    override def message =
      show"Unsafe to prune offset $offset due to the event for $domainId with record time $recordTime"
  }

  final case class LedgerPruningNotPossibleDuringHardMigration(
      domainId: DomainId,
      status: DomainConnectionConfigStore.Status,
  ) extends LedgerPruningError {
    override def message =
      s"The domain $domainId can not be pruned as there is a pending domain migration: $status"
  }

  final case class PurgingUnknownDomain(domainId: DomainId) extends LedgerPruningError {
    override def message = s"Domain $domainId does not exist."
  }

  final case class PurgingOnlyAllowedOnInactiveDomain(
      domainId: DomainId,
      status: DomainConnectionConfigStore.Status,
  ) extends LedgerPruningError {
    override def message: String =
      s"Domain $domainId status needs to be inactive, but is ${status.getClass.getSimpleName}"
  }
}
