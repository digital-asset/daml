// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.entries

import java.time.Instant

import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.{CommittedTransaction, NodeId}
import com.daml.ledger._
import com.daml.ledger.api.domain.RejectionReason

private[platform] sealed abstract class LedgerEntry extends Product with Serializable

private[platform] object LedgerEntry {

  final case class Rejection(
      recordTime: Instant,
      commandId: CommandId,
      applicationId: ApplicationId,
      actAs: List[Party],
      rejectionReason: RejectionReason)
      extends LedgerEntry

  final case class Transaction(
      commandId: Option[CommandId],
      transactionId: TransactionId,
      applicationId: Option[ApplicationId],
      actAs: List[Party],
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Instant,
      recordedAt: Instant,
      transaction: CommittedTransaction,
      explicitDisclosure: Relation[NodeId, Party])
      extends LedgerEntry
}
