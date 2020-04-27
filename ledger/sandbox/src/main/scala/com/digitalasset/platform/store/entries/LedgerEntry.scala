// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import java.time.Instant

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.GenTransaction
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.ledger._
import com.daml.ledger.api.domain.RejectionReason

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
}
