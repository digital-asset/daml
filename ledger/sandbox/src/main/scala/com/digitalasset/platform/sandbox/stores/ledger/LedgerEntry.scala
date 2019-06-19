// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
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
      transactionId: TransactionIdString,
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
