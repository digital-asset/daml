// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger._
import com.digitalasset.ledger.backend.api.v1.RejectionReason

sealed abstract class LedgerEntry extends Product with Serializable {
  def recordedAt: Instant

  def maybeCommandId: Option[CommandId]
}

//TODO: use domain types here, see: com.digitalasset.ledger.api.domain.*
object LedgerEntry {

  final case class Rejection(
      recordTime: Instant,
      commandId: CommandId,
      applicationId: ApplicationId,
      submitter: Party,
      rejectionReason: RejectionReason)
      extends LedgerEntry {
    override def maybeCommandId: Option[CommandId] = Some(commandId)

    override def recordedAt: Instant = recordTime
  }

  final case class Transaction(
      commandId: CommandId,
      transactionId: TransactionIdString,
      applicationId: ApplicationId,
      submittingParty: Party,
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Instant,
      recordedAt: Instant,
      transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId],
      explicitDisclosure: Relation[EventId, Party])
      extends LedgerEntry {
    override def maybeCommandId: Option[CommandId] = Some(commandId)
  }

  final case class Checkpoint(recordedAt: Instant) extends LedgerEntry {
    override def maybeCommandId: Option[CommandId] = None
  }

}
