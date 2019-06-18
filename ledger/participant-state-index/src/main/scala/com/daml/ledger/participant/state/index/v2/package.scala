// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.ledger.api.domain._

package object v2 {

  object AcsUpdateEvent {

    final case class Create(
        transactionId: TransactionId,
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Ref.Identifier,
        argument: Value.VersionedValue[Value.AbsoluteContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: Set[Ref.Party],
        contractKey: Option[Value.VersionedValue[Value.AbsoluteContractId]]
    )
  }

  final case class ActiveContractSetSnapshot(
      takenAt: LedgerOffset.Absolute,
      activeContracts: Source[(Option[WorkflowId], AcsUpdateEvent.Create), NotUsed])

  /** Information provided by the submitter of changes submitted to the ledger.
    *
    * Note that this is used for party-originating changes only. They are
    * usually issued via the Ledger API.
    *
    * @param submitter: the party that submitted the change.
    *
    * @param applicationId: an identifier for the DAML application that
    *   submitted the command. This is used for monitoring and to allow DAML
    *   applications subscribe to their own submissions only.
    *
    * @param commandId: a submitter provided identifier that he can use to
    *   correlate the stream of changes to the participant state with the
    *   changes he submitted.
    *
    */
  final case class SubmitterInfo(
      submitter: Ref.Party,
      applicationId: ApplicationId,
      commandId: CommandId
  )

  /** Meta-data of a transaction visible to all parties that can see a part of
    * the transaction.
    *
    * @param transactionId: identifier of the transaction for looking it up
    *   over the DAML Ledger API.
    *
    *   Implementors are free to make it equal to the 'offset' of this event.
    *
    * @param offset: The offset of this event, which uniquely identifies it.

    * @param ledgerEffectiveTime: the submitter-provided time at which the
    *   transaction should be interpreted. This is the time returned by the
    *   DAML interpreter on a `getTime :: Update Time` call.
    *
    * @param recordTime:
    *   The time at which this event was recorded. Depending on the
    *   implementation this time can be local to a Participant node or global
    *   to the whole ledger.
    *
    *
    * @param workflowId: a submitter-provided identifier used for monitoring
    *   and to traffic-shape the work handled by DAML applications
    *   communicating over the ledger. Meant to used in a coordinated
    *   fashion by all parties participating in the workflow.
    */
  final case class TransactionMeta(
      transactionId: TransactionId,
      offset: LedgerOffset.Absolute,
      ledgerEffectiveTime: Instant,
      recordTime: Instant,
      workflowId: WorkflowId)

  final case class LedgerConfiguration(minTTL: Duration, maxTTL: Duration)
}
