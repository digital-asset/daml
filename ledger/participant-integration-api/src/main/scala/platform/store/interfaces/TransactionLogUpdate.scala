// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interfaces

import java.time.Instant
import com.daml.lf.value.{Value => LfValue}

import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref.IdString
import com.daml.lf.ledger.EventId
import com.daml.platform.store.appendonlydao.events
import com.daml.platform.store.appendonlydao.events.{ContractId, Identifier}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId

trait TransactionLogUpdate extends Product with Serializable

object TransactionLogUpdate {
  final case class Transaction(
      transactionId: String,
      commandId: String,
      workflowId: String,
      effectiveAt: Instant,
      offset: Offset,
      lastEventSequentialId: EventSequentialId,
      events: Seq[Event],
  ) extends TransactionLogUpdate

  final case class LedgerEndMarker(eventOffset: Offset, eventSequentialId: EventSequentialId)
      extends TransactionLogUpdate

  sealed trait Event extends Product with Serializable {
    def eventOffset: Offset
    def eventSequentialId: EventSequentialId
    def transactionId: String
    def eventId: EventId
    def commandId: String
    def workflowId: String
    def ledgerEffectiveTime: Instant
    def flatEventWitnesses: Set[String]
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
      ledgerEffectiveTime: Instant,
      templateId: Identifier,
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue[events.ContractId]],
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      createArgument: LfValue.VersionedValue[events.ContractId],
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
      ledgerEffectiveTime: Instant,
      templateId: Identifier,
      commandId: String,
      workflowId: String,
      contractKey: Option[LfValue.VersionedValue[events.ContractId]],
      treeEventWitnesses: Set[String],
      flatEventWitnesses: Set[String],
      choice: String,
      actingParties: Set[IdString.Party],
      children: Seq[String],
      exerciseArgument: LfValue.VersionedValue[ContractId],
      exerciseResult: Option[LfValue.VersionedValue[ContractId]],
      consuming: Boolean,
  ) extends Event
}
