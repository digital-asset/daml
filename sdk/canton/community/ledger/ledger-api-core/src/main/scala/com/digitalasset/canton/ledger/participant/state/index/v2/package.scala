// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.digitalasset.canton.ledger.api.domain.*

import java.time.Duration

package v2 {

  import com.daml.lf.data.Time.Timestamp

  object AcsUpdateEvent {

    final case class Create(
        transactionId: TransactionId,
        eventId: EventId,
        contractId: Value.ContractId,
        templateId: Ref.Identifier,
        argument: Value.VersionedValue,
        stakeholders: Set[Ref.Party],
        contractKey: Option[Value.VersionedValue],
        signatories: Set[Ref.Party],
        observers: Set[Ref.Party],
        agreementText: String,
    )
  }

  /** Information provided by the submitter of changes submitted to the ledger.
    *
    * Note that this is used for party-originating changes only. They are
    * usually issued via the Ledger API.
    *
    * @param submitter: the party that submitted the change.
    *
    * @param applicationId: an identifier for the Daml application that
    *   submitted the command. This is used for monitoring and to allow Daml
    *   applications subscribe to their own submissions only.
    *
    * @param commandId: a submitter provided identifier that he can use to
    *   correlate the stream of changes to the participant state with the
    *   changes he submitted.
    */
  final case class SubmitterInfo(
      submitter: Ref.Party,
      applicationId: Ref.ApplicationId,
      commandId: CommandId,
  )

  /** Meta-data of a transaction visible to all parties that can see a part of
    * the transaction.
    *
    * @param transactionId: identifier of the transaction for looking it up
    *   over the Daml Ledger API.
    *
    *   Implementors are free to make it equal to the 'offset' of this event.
    *
    * @param offset: The offset of this event, which uniquely identifies it.
    *
    * @param ledgerEffectiveTime: the submitter-provided time at which the
    *   transaction should be interpreted. This is the time returned by the
    *   Daml interpreter on a `getTime :: Update Time` call.
    *
    * @param recordTime:
    *   The time at which this event was recorded. Depending on the
    *   implementation this time can be local to a Participant node or global
    *   to the whole ledger.
    *
    * @param workflowId: a submitter-provided identifier used for monitoring
    *   and to traffic-shape the work handled by Daml applications
    *   communicating over the ledger. Meant to used in a coordinated
    *   fashion by all parties participating in the workflow.
    */
  final case class TransactionMeta(
      transactionId: TransactionId,
      offset: LedgerOffset.Absolute,
      ledgerEffectiveTime: Timestamp,
      recordTime: Timestamp,
      workflowId: WorkflowId,
  )

  final case class LedgerConfiguration(maxDeduplicationDuration: Duration)

  /** Meta-data of a Daml-LF package
    *
    * @param size              : The size of the archive payload, in bytes.
    *
    * @param knownSince        : Indicates since when the package is known to
    *   the backing participant.
    *
    * @param sourceDescription : Optional description provided by the backing
    *   participant describing where it got the package from.
    */
  final case class PackageDetails(
      size: Long,
      knownSince: Timestamp,
      sourceDescription: Option[String],
  )
}
