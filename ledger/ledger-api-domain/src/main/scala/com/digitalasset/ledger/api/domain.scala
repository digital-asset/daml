// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger.api.domain.Event.{CreateOrArchiveEvent, CreateOrExerciseEvent}
import com.daml.ledger.configuration.Configuration
import com.daml.lf.command.{
  ClientProvidedContractMetadata,
  DisclosedContract,
  ApiCommands => LfCommands,
}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.data.Ref.LedgerString.ordering
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.logging._
import com.daml.lf.value.{Value => Lf}
import com.daml.logging.entries.LoggingValue.OfString
import com.daml.logging.entries.{LoggingValue, ToLoggingValue}
import scalaz.syntax.tag._
import scalaz.{@@, Tag}

import java.net.URL
import scala.collection.immutable
import scala.util.Try
import scala.util.control.NonFatal

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
      includeCreateArgumentsBlob: Boolean,
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

  type Value = Lf

  final case class RecordField(label: Option[Label], value: Value)

  sealed trait LabelTag

  type Label = String @@ LabelTag
  val Label: Tag.TagOf[LabelTag] = Tag.of[LabelTag]

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
      disclosedContracts: ImmArray[DisclosedContract[ClientProvidedContractMetadata]],
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
    def empty: ObjectMeta = ObjectMeta(
      resourceVersionO = None,
      annotations = Map.empty,
    )
  }

  final case class User(
      id: Ref.UserId,
      primaryParty: Option[Ref.Party],
      isDeactivated: Boolean = false,
      metadata: ObjectMeta = ObjectMeta.empty,
  )

  case class PartyDetails(
      party: Ref.Party,
      displayName: Option[String],
      isLocal: Boolean,
      metadata: ObjectMeta,
  )

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

  final case class JwksUrl(value: String) extends AnyVal {
    def toURL = new URL(value)
  }
  object JwksUrl {
    def fromString(value: String): Either[String, JwksUrl] =
      Try(new URL(value)).toEither.left
        .map { case NonFatal(e) =>
          e.getMessage
        }
        .map(_ => JwksUrl(value))

    def assertFromString(str: String): JwksUrl = fromString(str) match {
      case Right(value) => value
      case Left(err) => throw new IllegalArgumentException(err)
    }
  }

  sealed trait IdentityProviderId {
    def toRequestString: String

    def toDb: Option[IdentityProviderId.Id]
  }

  object IdentityProviderId {
    final case object Default extends IdentityProviderId {
      override def toRequestString: String = ""
      override def toDb: Option[Id] = None
    }

    final case class Id(value: Ref.LedgerString) extends IdentityProviderId {
      override def toRequestString: String = value

      override def toDb: Option[Id] = Some(this)
    }

    object Id {
      def fromString(id: String): Either[String, IdentityProviderId.Id] = {
        Ref.LedgerString.fromString(id).map(Id.apply)
      }

      def assertFromString(id: String): Id = {
        Id(Ref.LedgerString.assertFromString(id))
      }
    }

    def apply(identityProviderId: String): IdentityProviderId =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Id(Ref.LedgerString.assertFromString(id))
        case None => Default
      }

    def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
      Some(identityProviderId).filter(_.nonEmpty) match {
        case Some(id) => Ref.LedgerString.fromString(id).map(Id.apply)
        case None => Right(Default)
      }

    def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
      identityProviderId match {
        case None => IdentityProviderId.Default
        case Some(id) => id
      }
  }

  final case class IdentityProviderConfig(
      identityProviderId: IdentityProviderId.Id,
      isDeactivated: Boolean = false,
      jwksUrl: JwksUrl,
      issuer: String,
  )
}
