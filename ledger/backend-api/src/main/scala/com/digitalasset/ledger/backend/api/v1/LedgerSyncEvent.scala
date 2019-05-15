// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.backend.api.v1

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionId}
import com.digitalasset.daml.lf.data.Relation.Relation

sealed trait LedgerSyncEvent extends Product with Serializable {
  def offset: LedgerSyncOffset
  def recordTime: Instant
}

object LedgerSyncEvent {

  /** A transaction that has been accepted as committed by the Participant
    * node.
    *
    * @param recordTime:
    *   The time at which this event was recorded. Depending on the
    *   implementation this time can be local to a Participant node or global
    *   to the whole ledger.
    *
    * @param offset:
    *   The offset of this event, which uniquely identifies it.
    *
    * @param transactionId:
    *   identifier of the transaction for looking it up
    *   over the DAML Ledger API.
    *
    *   Implementors are free to make it equal to the 'offset' of this event.
    *
    * @param ledgerEffectiveTime:
    *   the ledger-effective time associated to the transaction by the
    *   submitter.
    *
    * @param transaction: the transaction that was accepted as committed.
    *
    * @param explicitDisclosure:
    *   a relation from nodes in the transaction to parties that can see that
    *   node. Used to filter by party in the DAML Ledger API.
    *
    * @param workflowId:
    *   The on-ledger workflow id that this transction advances. Provided by
    *   the submitter of the transaction, but meant to used in a coordinated
    *   fashion by all parties participating in the workflow.
    *
    * @param submitter:
    *   the party that submitted the transaction. Is [[None]] if the
    *   Participant node does not host the submitter.
    *
    * @param applicationId:
    *   An identifier to identify the DAML application that submitted
    *   this transaction. Used for monitoring. Is [[None]] if the Participant
    *   node does not host the submitter.
    *
    * @param commandId:
    *   A unique identifier of the command that produced the transaction. Used
    *   by the submitter to correlate the submission of a command with the
    *   acceptance or rejection of a transaction. In contrast to transactionId
    *   and offset this identifier is provided by the submitter and can
    *   therefore not be trusted to be unique by Participant nodes not hosting
    *   the submitter. Is [[None]] if the Participant
    *   node does not host the submitter.
    *
    */
  final case class AcceptedTransaction(
      transaction: CommittedTransaction,
      transactionId: TransactionId,
      submitter: Option[Party],
      ledgerEffectiveTime: Instant,
      recordTime: Instant,
      offset: LedgerSyncOffset,
      workflowId: String,
      explicitDisclosure: Relation[NodeId, Party],
      applicationId: Option[String] = None,
      commandId: Option[CommandId] = None
  ) extends LedgerSyncEvent

  /** A command that has been rejected by the Participant node.
    *
    * @param recordTime:
    *   The time at which this event was recorded. Depending on the
    *   implementation this time can be local to a Participant node or global
    *   to the whole ledger.
    *   to the whole ledger.
    *
    * @param offset:
    *   The offset of this event, which uniquely identifies it.
    *
    * @param submitter:
    *   the party that submitted the command. The intention is that a
    *   [[RejectedCommand]] event is only shown to the submitter.
    *
    * @param rejectionReason: reason for the rejection.
    *
    * @param workflowId:
    *   The on-ledger workflow id that this transction advances. Provided by
    *   the submitter of the transaction, but meant to used in a coordinated
    *   fashion by all parties participating in the workflow.
    *
    * @param applicationId:
    *   An identifier to identify the DAML application that submitted
    *   this transaction. Used for monitoring.
    *
    */
  final case class RejectedCommand(
      recordTime: Instant,
      commandId: CommandId,
      submitter: Party,
      rejectionReason: RejectionReason,
      offset: LedgerSyncOffset,
      applicationId: Option[String] = None // TODO should not be an option
  ) extends LedgerSyncEvent

  /** A heartbeat signalling that the ledger backend is online.
    *
    * @param recordTime:
    *   The time at which this event was recorded. Depending on the
    *   implementation this time can be local to a Participant node or global
    *   to the whole ledger.
    *
    * @param offset:
    *   The offset of this event, which uniquely identifies it.
    *
    * Note that [[Heartbeat]]s can also be used to signal a change to the
    * recordTime of the ledger.
    *
    */
  final case class Heartbeat(
      recordTime: Instant,
      offset: LedgerSyncOffset
  ) extends LedgerSyncEvent

}
