// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger.api.domain.Event.{CreateOrArchiveEvent, CreateOrExerciseEvent}
import com.daml.ledger.configuration.Configuration
import com.daml.lf.command.{DisclosedContract, ApiCommands => LfCommands}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Ref.LedgerString.ordering
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.logging._
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId => LfContractId}
import com.daml.lf.value.{Value => Lf}
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import scalaz.syntax.tag._
import scalaz.{@@, Tag}

import scala.collection.immutable

object domain {

  final case class TransactionFilter(filtersByParty: immutable.Map[Ref.Party, Filters]) {
    def apply(party: Ref.Party, template: Ref.Identifier): Boolean =
      filtersByParty.get(party).fold(false)(_.apply(template))
  }

  object TransactionFilter {

    /** These parties subscribe for all templates */
    def allForParties(parties: Set[Ref.Party]): TransactionFilter =
      TransactionFilter(parties.view.map(_ -> Filters.noFilter).toMap)
  }

  final case class Filters(inclusive: Option[InclusiveFilters]) {
    def apply(identifier: Ref.Identifier): Boolean =
      inclusive.fold(true)(_.templateIds.contains(identifier))
  }

  object Filters {
    val noFilter: Filters = Filters(None)

    def apply(inclusive: InclusiveFilters) = new Filters(Some(inclusive))
  }

  final case class InterfaceFilter(
      interfaceId: Ref.Identifier,
      includeView: Boolean,
  )

  final case class InclusiveFilters(
      templateIds: immutable.Set[Ref.Identifier],
      interfaceFilters: immutable.Set[InterfaceFilter],
  )

  sealed abstract class LedgerOffset extends Product with Serializable

  object LedgerOffset {

    final case class Absolute(value: Ref.LedgerString) extends LedgerOffset

    case object LedgerBegin extends LedgerOffset

    case object LedgerEnd extends LedgerOffset

    implicit val `Absolute Ordering`: Ordering[LedgerOffset.Absolute] =
      Ordering.by[LedgerOffset.Absolute, String](_.value)

    implicit val `LedgerOffset to LoggingValue`: ToLoggingValue[LedgerOffset] = value =>
      LoggingValue.OfString(value match {
        case LedgerOffset.Absolute(absolute) => absolute
        case LedgerOffset.LedgerBegin => "%begin%"
        case LedgerOffset.LedgerEnd => "%end%"
      })
  }

  sealed trait Event extends Product with Serializable {

    def eventId: EventId

    def contractId: ContractId

    def templateId: Ref.Identifier

    def witnessParties: immutable.Set[Ref.Party]

    def children: List[EventId] = Nil
  }

  object Event {

    sealed trait CreateOrExerciseEvent extends Event

    sealed trait CreateOrArchiveEvent extends Event

    final case class CreatedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Ref.Identifier,
        createArguments: Lf.ValueRecord,
        witnessParties: immutable.Set[Ref.Party],
        signatories: immutable.Set[Ref.Party],
        observers: immutable.Set[Ref.Party],
        agreementText: String,
        contractKey: Option[Value],
    ) extends Event
        with CreateOrExerciseEvent
        with CreateOrArchiveEvent

    final case class ArchivedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Ref.Identifier,
        witnessParties: immutable.Set[Ref.Party],
    ) extends Event
        with CreateOrArchiveEvent

    final case class ExercisedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Ref.Identifier,
        interfaceId: Option[Ref.Identifier],
        choice: Ref.ChoiceName,
        choiceArgument: Value,
        actingParties: immutable.Set[Ref.Party],
        consuming: Boolean,
        override val children: List[EventId],
        witnessParties: immutable.Set[Ref.Party],
        exerciseResult: Option[Value],
    ) extends Event
        with CreateOrExerciseEvent

  }

  sealed abstract class TransactionBase {

    def transactionId: TransactionId

    def commandId: Option[CommandId]

    def workflowId: Option[WorkflowId]

    def effectiveAt: Timestamp

    def offset: LedgerOffset.Absolute
  }

  final case class TransactionTree(
      transactionId: TransactionId,
      commandId: Option[CommandId],
      workflowId: Option[WorkflowId],
      effectiveAt: Timestamp,
      offset: LedgerOffset.Absolute,
      eventsById: immutable.Map[EventId, CreateOrExerciseEvent],
      rootEventIds: immutable.Seq[EventId],
  ) extends TransactionBase

  final case class Transaction(
      transactionId: TransactionId,
      commandId: Option[CommandId],
      workflowId: Option[WorkflowId],
      effectiveAt: Timestamp,
      events: immutable.Seq[CreateOrArchiveEvent],
      offset: LedgerOffset.Absolute,
  ) extends TransactionBase

  sealed trait CompletionEvent extends Product with Serializable {
    def offset: LedgerOffset.Absolute
    def recordTime: Timestamp
  }

  object CompletionEvent {

    final case class Checkpoint(offset: LedgerOffset.Absolute, recordTime: Timestamp)
        extends CompletionEvent

    final case class CommandAccepted(
        offset: LedgerOffset.Absolute,
        recordTime: Timestamp,
        commandId: CommandId,
        transactionId: TransactionId,
    ) extends CompletionEvent

    final case class CommandRejected(
        offset: LedgerOffset.Absolute,
        recordTime: Timestamp,
        commandId: CommandId,
        reason: RejectionReason,
    ) extends CompletionEvent
  }

  sealed trait RejectionReason {
    val description: String
  }

  object RejectionReason {
    final case class ContractsNotFound(missingContractIds: Set[String]) extends RejectionReason {
      override val description =
        s"Unknown contracts: ${missingContractIds.mkString("[", ", ", "]")}"
    }

    final case class InconsistentContractKeys(
        lookupResult: Option[LfContractId],
        currentResult: Option[LfContractId],
    ) extends RejectionReason {
      override val description: String =
        s"Contract key lookup with different results: expected [$lookupResult], actual [$currentResult]"
    }

    final case class DuplicateContractKey(key: GlobalKey) extends RejectionReason {
      override val description: String = "DuplicateKey: contract key is not unique"
    }

    /** The ledger time of the submission violated some constraint on the ledger time. */
    final case class InvalidLedgerTime(description: String) extends RejectionReason

    /** The transaction relied on contracts being active that were no longer
      * active at the point where it was sequenced.
      */
    final case class Inconsistent(description: String) extends RejectionReason

    /** The Participant node did not have sufficient resource quota with the
      * to submit the transaction.
      *
      * NOTE: Only used in Scala flyway migration scripts
      */
    final case class OutOfQuota(description: String) extends RejectionReason

    /** The transaction submission was disputed.
      *
      * This means that the underlying ledger and its validation logic
      * considered the transaction potentially invalid. This can be due to a bug
      * in the submission or validation logic, or due to malicious behaviour.
      *
      * NOTE: Only used in Scala flyway migration scripts
      */
    final case class Disputed(description: String) extends RejectionReason

    @deprecated
    final case class SubmitterCannotActViaParticipant(description: String) extends RejectionReason

    @deprecated
    final case class PartyNotKnownOnLedger(description: String) extends RejectionReason
  }

  type Value = Lf

  final case class RecordField(label: Option[Label], value: Value)

  sealed trait LabelTag

  type Label = String @@ LabelTag
  val Label: Tag.TagOf[LabelTag] = Tag.of[LabelTag]

  sealed trait VariantConstructorTag

  type VariantConstructor = String @@ VariantConstructorTag
  val VariantConstructor: Tag.TagOf[VariantConstructorTag] = Tag.of[VariantConstructorTag]

  sealed trait WorkflowIdTag

  type WorkflowId = Ref.WorkflowId @@ WorkflowIdTag
  val WorkflowId: Tag.TagOf[WorkflowIdTag] = Tag.of[WorkflowIdTag]

  sealed trait CommandIdTag

  type CommandId = Ref.CommandId @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  sealed trait TransactionIdTag

  type TransactionId = Ref.TransactionId @@ TransactionIdTag
  val TransactionId: Tag.TagOf[TransactionIdTag] = Tag.of[TransactionIdTag]

  sealed trait ContractIdTag

  type ContractId = Ref.ContractIdString @@ ContractIdTag
  val ContractId: Tag.TagOf[ContractIdTag] = Tag.of[ContractIdTag]

  sealed trait EventIdTag

  type EventId = Ref.LedgerString @@ EventIdTag
  val EventId: Tag.TagOf[EventIdTag] = Tag.of[EventIdTag]
  implicit val eventIdOrdering: Ordering[EventId] =
    Ordering.by[EventId, Ref.LedgerString](_.unwrap)

  sealed trait LedgerIdTag

  type LedgerId = String @@ LedgerIdTag
  val LedgerId: Tag.TagOf[LedgerIdTag] = Tag.of[LedgerIdTag]

  def optionalLedgerId(raw: String): Option[LedgerId] =
    if (raw.isEmpty) None else Some(LedgerId(raw))

  sealed trait ParticipantIdTag

  type ParticipantId = Ref.ParticipantId @@ ParticipantIdTag
  val ParticipantId: Tag.TagOf[ParticipantIdTag] = Tag.of[ParticipantIdTag]

  sealed trait SubmissionIdTag

  type SubmissionId = Ref.SubmissionId @@ SubmissionIdTag
  val SubmissionId: Tag.TagOf[SubmissionIdTag] = Tag.of[SubmissionIdTag]

  case class Commands(
      ledgerId: Option[LedgerId],
      workflowId: Option[WorkflowId],
      applicationId: Ref.ApplicationId,
      commandId: CommandId,
      submissionId: Option[SubmissionId],
      actAs: Set[Ref.Party],
      readAs: Set[Ref.Party],
      submittedAt: Timestamp,
      deduplicationPeriod: DeduplicationPeriod,
      commands: LfCommands,
      disclosedContracts: ImmArray[DisclosedContract],
  )

  object Commands {

    import Logging._

    implicit val `Timestamp to LoggingValue`: ToLoggingValue[Timestamp] =
      ToLoggingValue.ToStringToLoggingValue

    implicit val `Commands to LoggingValue`: ToLoggingValue[Commands] = commands => {
      val maybeString: Option[String] = commands.ledgerId.map(Tag.unwrap)
      LoggingValue.Nested.fromEntries(
        "ledgerId" -> OfString(maybeString.getOrElse("<empty-ledger-id>")),
        "workflowId" -> commands.workflowId,
        "applicationId" -> commands.applicationId,
        "submissionId" -> commands.submissionId,
        "commandId" -> commands.commandId,
        "actAs" -> commands.actAs,
        "readAs" -> commands.readAs,
        "submittedAt" -> commands.submittedAt,
        "deduplicationPeriod" -> commands.deduplicationPeriod,
      )
    }
  }

  /** Represents a party with additional known information.
    *
    * @param party       The stable unique identifier of a Daml party.
    * @param displayName Human readable name associated with the party. Might not be unique.
    * @param isLocal     True if party is hosted by the backing participant.
    */
  case class PartyDetails(party: Ref.Party, displayName: Option[String], isLocal: Boolean)

  sealed abstract class PartyEntry() extends Product with Serializable

  object PartyEntry {
    final case class AllocationAccepted(
        submissionId: Option[String],
        partyDetails: PartyDetails,
    ) extends PartyEntry

    final case class AllocationRejected(
        submissionId: String,
        reason: String,
    ) extends PartyEntry
  }

  /** Configuration entry describes a change to the current configuration. */
  sealed abstract class ConfigurationEntry extends Product with Serializable

  object ConfigurationEntry {
    final case class Accepted(
        submissionId: String,
        configuration: Configuration,
    ) extends ConfigurationEntry

    final case class Rejected(
        submissionId: String,
        rejectionReason: String,
        proposedConfiguration: Configuration,
    ) extends ConfigurationEntry
  }

  sealed abstract class PackageEntry() extends Product with Serializable

  object PackageEntry {
    final case class PackageUploadAccepted(
        submissionId: String,
        recordTime: Timestamp,
    ) extends PackageEntry

    final case class PackageUploadRejected(
        submissionId: String,
        recordTime: Timestamp,
        reason: String,
    ) extends PackageEntry
  }

  object Logging {
    implicit def `tagged value to LoggingValue`[T: ToLoggingValue, Tag]: ToLoggingValue[T @@ Tag] =
      value => value.unwrap
  }

  final case class ObjectMeta(
      resourceVersionO: Option[Long],
      annotations: Map[String, String],
  )

  object ObjectMeta {
    // TODO um-for-hub: Review usage
    def empty: ObjectMeta = ObjectMeta(
      resourceVersionO = None,
      annotations = Map.empty,
    )
  }

  final case class User(
      id: Ref.UserId,
      primaryParty: Option[Ref.Party],
      // TODO um-for-hub: Remove default values
      // NOTE: Do not set 'isDeactivated' and 'metadata'. These are work-in-progress features.
      isDeactivated: Boolean = false,
      metadata: ObjectMeta = ObjectMeta.empty,
  )

  // TODO um-for-hub: Drop redundant ParticipantParty object
  object ParticipantParty {

    final case class PartyRecord(
        party: Ref.Party,
        metadata: ObjectMeta,
    )

  }

  sealed abstract class UserRight extends Product with Serializable
  object UserRight {
    final case object ParticipantAdmin extends UserRight
    final case class CanActAs(party: Ref.Party) extends UserRight
    final case class CanReadAs(party: Ref.Party) extends UserRight
  }

  sealed abstract class Feature extends Product with Serializable
  object Feature {
    case object UserManagement extends Feature
  }
}
