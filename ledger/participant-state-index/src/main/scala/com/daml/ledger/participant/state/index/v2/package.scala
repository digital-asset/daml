// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import java.time.{Duration, Instant}

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.daml.ledger.api.domain._

package v2 {

  object AcsUpdateEvent {

    final case class Create(
        transactionId: TransactionId,
        eventId: EventId,
        contractId: Value.ContractId,
        templateId: Ref.Identifier,
        argument: Value.VersionedValue[Value.ContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: Set[Ref.Party],
        contractKey: Option[Value.VersionedValue[Value.ContractId]],
        signatories: Set[Ref.Party],
        observers: Set[Ref.Party],
        agreementText: String,
    )
  }

  final case class ActiveContractSetSnapshot(
      takenAt: LedgerOffset.Absolute,
      activeContracts: Source[(Option[WorkflowId], AcsUpdateEvent.Create), NotUsed],
  )

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
      applicationId: ApplicationId,
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
      ledgerEffectiveTime: Instant,
      recordTime: Instant,
      workflowId: WorkflowId,
  )

  final case class LedgerConfiguration(maxDeduplicationTime: Duration)

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
      knownSince: Instant,
      sourceDescription: Option[String],
  )

  sealed abstract class CommandDeduplicationResult extends Product with Serializable

  /** This is the first time the command was submitted. */
  case object CommandDeduplicationNew extends CommandDeduplicationResult

  /** This command was submitted before. */
  final case class CommandDeduplicationDuplicate(deduplicateUntil: Instant)
      extends CommandDeduplicationResult

  sealed abstract class InitializationResult
  object InitializationResult {

    /** The stored ledgerId and participantId both contained the expected value. */
    case object AlreadyExists extends InitializationResult

    /** Either the stored ledgerId or the stored participantId contained an unexpected value. */
    final case class Mismatch(
        existingLedgerId: LedgerId,
        existingParticipantId: Option[ParticipantId],
    ) extends InitializationResult

    /** The stored ledgerId and/or participantId was empty. It was updated to the expected value. */
    case object New extends InitializationResult
  }
}
