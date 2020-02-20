// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import com.daml.ledger.participant.state.v1.TransactionId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.{ApplicationId, CommandId}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.CompletionEvent
import com.digitalasset.ledger.api.domain.CompletionEvent.{CommandAccepted, CommandRejected}
import com.digitalasset.platform.store.dao.LedgerDao
import com.digitalasset.platform.store.entries.LedgerEntry

// Turn a stream of transactions into a stream of completions for a given application and set of parties
// TODO Remove this when:
// TODO - the participant can read completions off the index directly AND
// TODO - the in-memory sandbox is gone
private[platform] object CompletionFromTransaction {

  private def toApiOffset(offset: LedgerDao#LedgerOffset): domain.LedgerOffset.Absolute =
    domain.LedgerOffset.Absolute(Ref.LedgerString.assertFromString(offset.toString))

  private def toApiCommandId(commandId: CommandId): domain.CommandId =
    domain.CommandId(commandId)

  private def toApiTransactionId(transactionId: TransactionId): domain.TransactionId =
    domain.TransactionId(transactionId)

  // Filter completions for transactions for which we have the full submitter information: appId, submitter, cmdId
  // This doesn't make a difference for the sandbox (because it represents the ledger backend + api server in single package).
  // But for an api server that is part of a distributed ledger network, we might see
  // transactions that originated from some other api server. These transactions don't contain the submitter information,
  // and therefore we don't emit CommandAccepted completions for those
  def apply(appId: ApplicationId, parties: Set[Ref.Party]): PartialFunction[
    (LedgerDao#LedgerOffset, LedgerEntry),
    (LedgerDao#LedgerOffset, CompletionEvent)] = {
    case (
        offset,
        LedgerEntry.Transaction(
          Some(cmdId),
          transactionId,
          Some(`appId`),
          Some(submitter),
          _,
          _,
          recordTime,
          _,
          _)) if parties(submitter) =>
      offset -> CommandAccepted(
        toApiOffset(offset),
        recordTime,
        toApiCommandId(cmdId),
        toApiTransactionId(transactionId))
    case (offset, LedgerEntry.Rejection(recordTime, commandId, `appId`, submitter, rejectionReason))
        if parties(submitter) =>
      offset -> CommandRejected(
        toApiOffset(offset),
        recordTime,
        toApiCommandId(commandId),
        rejectionReason)
    case (offset, LedgerEntry.Checkpoint(recordedAt)) =>
      offset -> CompletionEvent.Checkpoint(toApiOffset(offset), recordedAt)
  }

}
