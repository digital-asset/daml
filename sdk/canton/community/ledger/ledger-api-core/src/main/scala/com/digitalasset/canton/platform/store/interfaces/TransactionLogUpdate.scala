// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interfaces

import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.TransactionShape.{AcsDelta, LedgerEffects}
import com.digitalasset.canton.ledger.participant.state.ReassignmentInfo
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel
import com.digitalasset.canton.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.digitalasset.canton.platform.{ContractId, Identifier}
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageName, Party}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value as LfValue

/** Generic ledger update event.
  *
  * Used as data source template for in-memory fan-out buffers for Ledger API streams serving.
  */
sealed trait TransactionLogUpdate extends Product with Serializable with HasTraceContext {
  def offset: Offset
}

object TransactionLogUpdate {

  /** Complete view of a ledger transaction.
    *
    * @param updateId The transaction it.
    * @param workflowId The workflow id.
    * @param effectiveAt The transaction ledger time.
    * @param offset The transaction's offset in the ledger.
    * @param events The transaction events, in execution order.
    * @param completionStreamResponse The successful submission's completion details.
    * @param recordTime The time at which the transaction was recorded.
    */
  final case class TransactionAccepted(
      updateId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      offset: Offset,
      events: Vector[Event],
      completionStreamResponse: Option[CompletionStreamResponse],
      synchronizerId: String,
      recordTime: Timestamp,
  )(implicit override val traceContext: TraceContext)
      extends TransactionLogUpdate

  /** A rejected submission.
    *
    * @param offset The offset at which the rejection has been enqueued in the ledger.
    * @param completionStreamResponse The rejected submission's completion details.
    */
  final case class TransactionRejected(
      offset: Offset,
      completionStreamResponse: CompletionStreamResponse,
  )(implicit override val traceContext: TraceContext)
      extends TransactionLogUpdate

  final case class ReassignmentAccepted(
      updateId: String,
      commandId: String,
      workflowId: String,
      offset: Offset,
      recordTime: Timestamp,
      completionStreamResponse: Option[CompletionStreamResponse],
      reassignmentInfo: ReassignmentInfo,
      reassignment: ReassignmentAccepted.Reassignment,
  )(implicit override val traceContext: TraceContext)
      extends TransactionLogUpdate {

    def stakeholders: List[Ref.Party] = reassignment match {
      case TransactionLogUpdate.ReassignmentAccepted.Assigned(createdEvent) =>
        createdEvent.flatEventWitnesses.toList

      case TransactionLogUpdate.ReassignmentAccepted.Unassigned(unassign) =>
        unassign.stakeholders
    }
  }

  final case class TopologyTransactionEffective(
      updateId: String,
      offset: Offset,
      effectiveTime: Timestamp,
      synchronizerId: String,
      events: Vector[PartyToParticipantAuthorization],
  )(implicit override val traceContext: TraceContext)
      extends TransactionLogUpdate

  object ReassignmentAccepted {
    sealed trait Reassignment
    final case class Assigned(createdEvent: CreatedEvent) extends Reassignment
    final case class Unassigned(
        unassign: com.digitalasset.canton.ledger.participant.state.Reassignment.Unassign
    ) extends Reassignment
  }

  /* Models all but divulgence events */
  sealed trait Event extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: EventSequentialId
    def updateId: String
    def commandId: String
    def workflowId: String
    def ledgerEffectiveTime: Timestamp
    def treeEventWitnesses: Set[Party]
    def flatEventWitnesses: Set[Party]
    def witnesses(transactionShape: TransactionShape): Set[Party]
    def submitters: Set[Party]
    def templateId: Identifier
    def contractId: ContractId
  }

  final case class CreatedEvent(
      eventOffset: Offset,
      updateId: String,
      nodeId: Int,
      eventSequentialId: Long,
      contractId: ContractId,
      ledgerEffectiveTime: Timestamp,
      templateId: Identifier,
      packageName: PackageName,
      packageVersion: Option[Ref.PackageVersion],
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue],
      treeEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
      submitters: Set[Party],
      createArgument: LfValue.VersionedValue,
      createSignatories: Set[Party],
      createObservers: Set[Party],
      createKeyHash: Option[Hash],
      createKey: Option[GlobalKey],
      createKeyMaintainers: Option[Set[Party]],
      driverMetadata: Bytes,
  ) extends Event {
    def witnesses(transactionShape: TransactionShape): Set[Party] =
      transactionShape match {
        case AcsDelta => flatEventWitnesses
        case LedgerEffects => treeEventWitnesses
      }
  }

  final case class ExercisedEvent(
      eventOffset: Offset,
      updateId: String,
      nodeId: Int,
      eventSequentialId: Long,
      contractId: ContractId,
      ledgerEffectiveTime: Timestamp,
      templateId: Identifier,
      packageName: PackageName,
      interfaceId: Option[Identifier],
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue],
      treeEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
      submitters: Set[Party],
      choice: String,
      actingParties: Set[Party],
      lastDescendantNodeId: Int,
      exerciseArgument: LfValue.VersionedValue,
      exerciseResult: Option[LfValue.VersionedValue],
      consuming: Boolean,
  ) extends Event {
    def witnesses(transactionShape: TransactionShape): Set[Party] =
      transactionShape match {
        case AcsDelta => flatEventWitnesses
        case LedgerEffects => treeEventWitnesses
      }
  }

  final case class PartyToParticipantAuthorization(
      party: Party,
      participant: Ref.ParticipantId,
      level: AuthorizationLevel,
  )

}
