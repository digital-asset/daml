// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.entries

import com.digitalasset.daml.lf.data.Relation
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, NodeId}
import com.digitalasset.canton.platform.*

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
