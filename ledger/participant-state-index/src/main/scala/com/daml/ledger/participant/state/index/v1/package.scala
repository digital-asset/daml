// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.value.Value

package object v1 {
  import com.daml.ledger.participant.state.v1._

  type TransactionAccepted = Update.TransactionAccepted
  type TransactionUpdate = (Offset, (Update.TransactionAccepted, BlindingInfo))

  /** ACS event identifier */
  type EventId = Ref.PackageId

  final case class AcsUpdate(
      optSubmitterInfo: Option[SubmitterInfo],
      offset: Offset,
      transactionMeta: TransactionMeta,
      transactionId: TransactionId,
      events: List[AcsUpdateEvent]
  )

  sealed trait AcsUpdateEvent extends Product with Serializable
  object AcsUpdateEvent {
    final case class Create(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Ref.DefinitionRef,
        argument: Value.VersionedValue[Value.AbsoluteContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent

    final case class Archive(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Ref.DefinitionRef,
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent
  }

  sealed trait CompletionEvent extends Product with Serializable {
    def offset: Offset
  }
  object CompletionEvent {
    final case class Checkpoint(offset: Offset, recordTime: Timestamp) extends CompletionEvent
    final case class CommandAccepted(
        offset: Offset,
        commandId: CommandId,
        transactionId: TransactionId)
        extends CompletionEvent
    final case class CommandRejected(offset: Offset, commandId: CommandId, reason: RejectionReason)
        extends CompletionEvent
  }

  final case class ActiveContractSetSnapshot(
      takenAt: Offset,
      activeContracts: Source[(WorkflowId, AcsUpdateEvent.Create), NotUsed])
}
