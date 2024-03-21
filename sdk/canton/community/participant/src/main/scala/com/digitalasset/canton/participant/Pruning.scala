// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.participant.store.{DomainConnectionConfigStore, EventLogId}
import com.digitalasset.canton.participant.sync.UpstreamOffsetConvert
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

  case object LedgerPruningOnlySupportedInEnterpriseEdition extends LedgerPruningError {
    val message = "Pruning of internal participant data only available in the Enterprise Edition."
  }

  final case class LedgerPruningOffsetUnsafeDomain(domain: DomainId) extends LedgerPruningError {
    override def message =
      s"No safe-to-prune offset for domain $domain."
  }

  final case class LedgerPruningOffsetUnsafeToPrune(
      globalOffset: GlobalOffset,
      eventLog: EventLogId,
      localOffset: LocalOffset,
      cause: String,
      lastSafeOffset: Option[GlobalOffset],
  ) extends LedgerPruningError {
    override def message =
      show"Unsafe to prune offset ${UpstreamOffsetConvert.fromGlobalOffset(globalOffset)} due to the event in $eventLog with local offset $localOffset"
  }

  final case class LedgerPruningOffsetNonCantonFormat(message: String) extends LedgerPruningError

  final case class LedgerPruningNotPossibleDuringHardMigration(
      domainId: DomainId,
      status: DomainConnectionConfigStore.Status,
  ) extends LedgerPruningError {
    override def message =
      s"The domain ${domainId} can not be pruned as there is a pending domain migration: ${status}"
  }

}
