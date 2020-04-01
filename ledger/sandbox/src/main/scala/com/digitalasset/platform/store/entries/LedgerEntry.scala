// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.entries

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger._
import com.digitalasset.ledger.api.domain.RejectionReason

sealed abstract class LedgerEntry extends Product with Serializable

object LedgerEntry {

  final case class Rejection(
      recordTime: Instant,
      commandId: CommandId,
      applicationId: ApplicationId,
      submitter: Party,
      rejectionReason: RejectionReason)
      extends LedgerEntry

  final case class Transaction(
      commandId: Option[CommandId],
      transactionId: TransactionId,
      applicationId: Option[ApplicationId],
      submittingParty: Option[Party],
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Instant,
      recordedAt: Instant,
      transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId],
      explicitDisclosure: Relation[EventId, Party])
      extends LedgerEntry

  final case class Checkpoint(recordedAt: Instant) extends LedgerEntry

}
