// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain.LedgerOffset

package object v1 {

  import com.daml.ledger.participant.state.v1._

  type TransactionAccepted = Update.TransactionAccepted
  type TransactionUpdate = (Offset, (Update.TransactionAccepted, BlindingInfo))

  /** ACS event identifier */
  type EventId = String

  final case class AcsUpdate(
      optSubmitterInfo: Option[SubmitterInfo],
      offset: Offset,
      transactionMeta: TransactionMeta,
      transactionId: TransactionIdString,
      events: List[AcsUpdateEvent]
  )

  sealed trait AcsUpdateEvent extends Product with Serializable {
    def stakeholders: Set[Party]

    def templateId: Ref.Identifier
  }

  object AcsUpdateEvent {

    final case class Create(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Ref.Identifier,
        contractKey: Option[KeyWithMaintainers[Value.VersionedValue[Value.AbsoluteContractId]]],
        argument: Value.VersionedValue[Value.AbsoluteContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: Set[Party],
    ) extends AcsUpdateEvent

    final case class Archive(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Ref.Identifier,
        // TODO(JM,SM): understand witnessing parties
        stakeholders: Set[Party],
    ) extends AcsUpdateEvent

  }

  sealed abstract class CompletionEvent extends Product with Serializable {
    def offset: Offset
  }

  object CompletionEvent {

    final case class Checkpoint(offset: Offset, recordTime: Timestamp) extends CompletionEvent

    final case class CommandAccepted(
        offset: Offset,
        commandId: CommandId,
        transactionId: TransactionIdString)
        extends CompletionEvent

    final case class CommandRejected(offset: Offset, commandId: CommandId, reason: RejectionReason)
        extends CompletionEvent

  }

  final case class ActiveContractSetSnapshot(
      takenAt: LedgerOffset.Absolute,
      activeContracts: Source[(Option[WorkflowId], AcsUpdateEvent.Create), NotUsed])

}
