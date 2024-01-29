// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.ledger.participant.state.v2.Update
import com.digitalasset.canton.participant.admin.workflows.java.canton
import com.digitalasset.canton.participant.protocol.ProcessingSteps.RequestType
import com.digitalasset.canton.protocol.LedgerTransactionNodeStatistics

final class EventTranslationStrategy(
    multiDomainLedgerAPIEnabled: Boolean,
    excludeInfrastructureTransactions: Boolean,
) {

  def translate(e: LedgerSyncEvent): Option[Update] =
    e match {
      case e: LedgerSyncEvent.TransferredOut =>
        if (multiDomainLedgerAPIEnabled) e.toDamlUpdate else None
      case e: LedgerSyncEvent.TransferredIn =>
        val transferInUpdate = if (multiDomainLedgerAPIEnabled) e.toDamlUpdate else None

        transferInUpdate.orElse(e.asTransactionAccepted)
      case e: LedgerSyncEvent.CommandRejected =>
        e.kind match {
          case RequestType.TransferIn | RequestType.TransferOut =>
            if (multiDomainLedgerAPIEnabled) e.toDamlUpdate else None
          case RequestType.Transaction =>
            e.toDamlUpdate
        }
      case e: LedgerSyncEvent.TransactionAccepted =>
        augmentTransactionStatistics(e).toDamlUpdate
      case e => e.toDamlUpdate
    }

  // Augment event with transaction statistics "as late as possible" as stats are redundant data and so that
  // we don't need to persist stats and deal with versioning stats changes. Also every event is usually consumed
  // only once.
  private[sync] def augmentTransactionStatistics(
      e: LedgerSyncEvent.TransactionAccepted
  ): LedgerSyncEvent.TransactionAccepted =
    e.copy(completionInfoO =
      e.completionInfoO.map(completionInfo =>
        completionInfo.copy(statistics =
          Some(LedgerTransactionNodeStatistics(e.transaction, excludedPackageIds))
        )
      )
    )

  private val excludedPackageIds: Set[LfPackageId] =
    if (excludeInfrastructureTransactions) {
      Set(
        canton.internal.ping.Ping.TEMPLATE_ID,
        canton.internal.bong.BongProposal.TEMPLATE_ID,
        canton.internal.bong.Bong.TEMPLATE_ID,
        canton.internal.bong.Merge.TEMPLATE_ID,
        canton.internal.bong.Explode.TEMPLATE_ID,
        canton.internal.bong.Collapse.TEMPLATE_ID,
      ).map(x => LfPackageId.assertFromString(x.getPackageId))
    } else {
      Set.empty[LfPackageId]
    }

}
