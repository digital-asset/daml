// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import com.daml.lf.data.Relation
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{CommittedTransaction, NodeId}
import com.daml.platform.{ApplicationId, CommandId, Party, SubmissionId, TransactionId, WorkflowId}

private[platform] sealed abstract class LedgerEntry extends Product with Serializable

private[platform] object LedgerEntry {

  final case class Transaction(
      commandId: Option[CommandId],
      transactionId: TransactionId,
      applicationId: Option[ApplicationId],
      submissionId: Option[SubmissionId],
      actAs: List[Party],
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Timestamp,
      recordedAt: Timestamp,
      transaction: CommittedTransaction,
      explicitDisclosure: Relation[NodeId, Party],
  ) extends LedgerEntry
}
