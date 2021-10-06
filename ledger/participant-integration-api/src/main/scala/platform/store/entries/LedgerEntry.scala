// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import com.daml.ledger.api.domain.RejectionReason
import com.daml.lf.data.Ref
import com.daml.lf.data.Relation.Relation
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{CommittedTransaction, NodeId}

private[platform] sealed abstract class LedgerEntry extends Product with Serializable

private[platform] object LedgerEntry {

  final case class Rejection(
      recordTime: Timestamp,
      commandId: Ref.CommandId,
      applicationId: Ref.ApplicationId,
      submissionId: Ref.SubmissionId,
      actAs: List[Ref.Party],
      rejectionReason: RejectionReason,
  ) extends LedgerEntry

  final case class Transaction(
      commandId: Option[Ref.CommandId],
      transactionId: Ref.TransactionId,
      applicationId: Option[Ref.ApplicationId],
      submissionId: Option[Ref.SubmissionId],
      actAs: List[Ref.Party],
      workflowId: Option[Ref.WorkflowId],
      ledgerEffectiveTime: Timestamp,
      recordedAt: Timestamp,
      transaction: CommittedTransaction,
      explicitDisclosure: Relation[NodeId, Ref.Party],
  ) extends LedgerEntry
}
