// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interfaces

import com.daml.ledger.offset.Offset
import com.daml.lf.value.{Value => LfValue}
import com.daml.lf.data.Ref.IdString
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.ledger.EventId
import com.daml.platform.{ContractId, Identifier}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId

/** Generic ledger update event.
  *
  * Used as data source template for in-memory fan-out buffers for Ledger API streams serving.
  *
  * @see [[LedgerDaoTransactionsReader.getTransactionLogUpdates()]]
  */
sealed trait TransactionLogUpdate extends Product with Serializable

object TransactionLogUpdate {

  /** Complete view of a ledger transaction.
    *
    * @param transactionId The transaction it.
    * @param commandId The command id.
    * @param workflowId The workflow id.
    * @param effectiveAt The transaction ledger time.
    * @param offset The transaction's offset in the ledger.
    * @param events The transaction events, in execution order.
    */
  final case class Transaction(
      transactionId: String,
      workflowId: String,
      effectiveAt: Timestamp,
      offset: Offset,
      events: Vector[Event],
  ) extends TransactionLogUpdate {
    require(events.nonEmpty, "Transaction must have at least an event")
  }

  /** A special event which signifies that the ledger end has been reached in a stream.
    *
    * @see [[LedgerDaoTransactionsReader.getTransactionLogUpdates()]]
    * @param eventOffset The ledger end offset.
    * @param eventSequentialId The ledger end event sequential id.
    */
  final case class LedgerEndMarker(eventOffset: Offset, eventSequentialId: EventSequentialId)
      extends TransactionLogUpdate

  /* Models all but divulgence events */
  sealed trait Event extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: EventSequentialId
    def transactionId: String
    def eventId: EventId
    def commandId: String
    def workflowId: String
    def ledgerEffectiveTime: Timestamp
    def treeEventWitnesses: Set[String]
    def flatEventWitnesses: Set[String]
    def submitters: Set[String]
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
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      submitters: Set[String],
      createArgument: LfValue.VersionedValue,
      createSignatories: Set[String],
      createObservers: Set[String],
      createAgreementText: Option[String],
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
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      submitters: Set[String],
      choice: String,
      actingParties: Set[IdString.Party],
      children: Seq[String],
      exerciseArgument: LfValue.VersionedValue,
      exerciseResult: Option[LfValue.VersionedValue],
      consuming: Boolean,
  ) extends Event
}
