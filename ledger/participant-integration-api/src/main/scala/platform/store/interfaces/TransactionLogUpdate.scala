// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interfaces

import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.lf.value.{Value => LfValue}
import com.daml.platform.{ContractId, Identifier}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Bytes

/** Generic ledger update event.
  *
  * Used as data source template for in-memory fan-out buffers for Ledger API streams serving.
  */
sealed trait TransactionLogUpdate extends Product with Serializable {
  def offset: Offset
}

object TransactionLogUpdate {

  /** Complete view of a ledger transaction.
    *
    * @param transactionId The transaction it.
    * @param workflowId The workflow id.
    * @param effectiveAt The transaction ledger time.
    * @param offset The transaction's offset in the ledger.
    * @param events The transaction events, in execution order.
    * @param completionDetails The successful submission's completion details.
    */
  final case class TransactionAccepted(
      transactionId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      offset: Offset,
      events: Vector[Event],
      completionDetails: Option[CompletionDetails],
  ) extends TransactionLogUpdate

  /** A rejected submission.
    *
    * @param offset The offset at which the rejection has been enqueued in the ledger.
    * @param completionDetails The rejected submission's completion details.
    */
  final case class TransactionRejected(
      offset: Offset,
      completionDetails: CompletionDetails,
  ) extends TransactionLogUpdate

  /** The transaction's completion details.
    *
    * @param completionStreamResponse The completion details
    * @param submitters The original command submitters
    */
  final case class CompletionDetails(
      completionStreamResponse: CompletionStreamResponse,
      submitters: Set[String],
  )

  /* Models all but divulgence events */
  sealed trait Event extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: EventSequentialId
    def transactionId: String
    def eventId: EventId
    def commandId: String
    def workflowId: String
    def ledgerEffectiveTime: Timestamp
    def treeEventWitnesses: Set[Party]
    def flatEventWitnesses: Set[Party]
    def submitters: Set[Party]
    def templateId: Identifier
    def contractId: ContractId
  }

  final case class CreatedEvent(
      eventOffset: Offset,
      transactionId: String,
      nodeIndex: Int,
      eventSequentialId: Long,
      eventId: EventId,
      contractId: ContractId,
      ledgerEffectiveTime: Timestamp,
      templateId: Identifier,
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue],
      treeEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
      submitters: Set[Party],
      createArgument: LfValue.VersionedValue,
      createSignatories: Set[Party],
      createObservers: Set[Party],
      createAgreementText: Option[String],
      createKeyHash: Option[Hash],
      driverMetadata: Bytes,
  ) extends Event

  final case class ExercisedEvent(
      eventOffset: Offset,
      transactionId: String,
      nodeIndex: Int,
      eventSequentialId: Long,
      eventId: EventId,
      contractId: ContractId,
      ledgerEffectiveTime: Timestamp,
      templateId: Identifier,
      interfaceId: Option[Identifier],
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue],
      treeEventWitnesses: Set[Party],
      flatEventWitnesses: Set[Party],
      submitters: Set[Party],
      choice: String,
      actingParties: Set[Party],
      children: Seq[String],
      exerciseArgument: LfValue.VersionedValue,
      exerciseResult: Option[LfValue.VersionedValue],
      consuming: Boolean,
  ) extends Event
}
