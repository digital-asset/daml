// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api

import java.time.Instant

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.SortedMap
import com.digitalasset.ledger.api.domain.Event.{CreateOrArchiveEvent, CreateOrExerciseEvent}
import com.digitalasset.ledger.api.domain.Value.RecordValue
import scalaz.{@@, Tag}

import scala.collection.{breakOut, immutable}

object domain {

  final case class Identifier(packageId: PackageId, moduleName: String, entityName: String)

  final case class TransactionFilter(filtersByParty: immutable.Map[Party, Filters])

  object TransactionFilter {

    /** These parties subscribe for all templates */
    def allForParties(parties: Set[Party]) =
      TransactionFilter(parties.map(_ -> Filters.noFilter)(breakOut))
  }

  final case class Filters(inclusive: Option[InclusiveFilters]) {
    def containsTemplateId(identifier: Identifier): Boolean =
      inclusive.fold(true)(_.templateIds.contains(identifier))
  }

  object Filters {
    val noFilter = Filters(None)

    def apply(inclusive: InclusiveFilters) = new Filters(Some(inclusive))
  }

  final case class InclusiveFilters(templateIds: immutable.Set[Identifier])

  sealed abstract class LedgerOffset extends Product with Serializable

  object LedgerOffset {

    final case class Absolute(value: String) extends LedgerOffset

    case object LedgerBegin extends LedgerOffset

    case object LedgerEnd extends LedgerOffset

  }

  sealed trait Event extends Product with Serializable {

    def eventId: EventId

    def contractId: ContractId

    def templateId: Identifier

    def witnessParties: immutable.Set[Party]
  }

  object Event {

    sealed trait CreateOrExerciseEvent extends Event

    sealed trait CreateOrArchiveEvent extends Event

    final case class CreatedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Identifier,
        createArguments: RecordValue,
        witnessParties: immutable.Set[Party])
        extends Event
        with CreateOrExerciseEvent
        with CreateOrArchiveEvent

    final case class ArchivedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Identifier,
        witnessParties: immutable.Set[Party])
        extends Event
        with CreateOrExerciseEvent

    final case class ExercisedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Identifier,
        contractCreatingEventId: EventId,
        choice: Choice,
        choiceArgument: Value,
        actingParties: immutable.Set[Party],
        consuming: Boolean,
        children: Event,
        witnessParties: immutable.Set[Party])
        extends Event
        with CreateOrArchiveEvent

  }

  sealed abstract class TransactionBase {

    def transactionId: TransactionId

    def commandId: CommandId

    def workflowId: WorkflowId

    def effectiveAt: Instant

    def events: immutable.Seq[Event]

    def offset: AbsoluteOffset

    def traceContext: Option[TraceContext]
  }

  final case class TransactionTree(
      transactionId: TransactionId,
      commandId: CommandId,
      workflowId: WorkflowId,
      effectiveAt: Instant,
      events: immutable.Seq[CreateOrArchiveEvent],
      offset: AbsoluteOffset,
      traceContext: Option[TraceContext])
      extends TransactionBase

  final case class Transaction(
      transactionId: TransactionId,
      commandId: CommandId,
      workflowId: WorkflowId,
      effectiveAt: Instant,
      events: immutable.Seq[CreateOrExerciseEvent],
      offset: AbsoluteOffset,
      traceContext: Option[TraceContext])
      extends TransactionBase

  sealed abstract class Value extends Product with Serializable

  object Value {

    final case class ListValue(elements: immutable.Seq[Value]) extends Value

    final case class VariantValue(
        variantId: Option[Identifier],
        variantConstructor: VariantConstructor,
        value: Value)
        extends Value

    final case class RecordValue(recordId: Option[Identifier], fields: immutable.Seq[RecordField])
        extends Value

    final case class ContractIdValue(contractId: ContractId) extends Value

    final case class Int64Value(int64: Long) extends Value

    final case class DecimalValue(value: String) extends Value

    final case class TextValue(text: String) extends Value

    final case class PartyValue(party: Party) extends Value

    final case class BoolValue(bool: Boolean) extends Value

    final case class TimeStampValue(microsSinceEpoch: Long) extends Value

    final case class DateValue(daysSinceEpoch: Int) extends Value

    final case class OptionalValue(opt: Option[Value]) extends Value { //TODO DEL-7054 test coverage
      def isEmpty = opt.isEmpty
    }
    object OptionalValue {
      val Empty = new OptionalValue(None)

      def apply(opt: Option[Value]): OptionalValue =
        opt.fold {
          Empty
        } { v =>
          if (v != null)
            new OptionalValue(opt)
          else
            Empty
        }
    }

    final case class MapValue(map: SortedMap[Value]) extends Value

    case object UnitValue extends Value

  }

  final case class CommandStatus(
      code: Int,
      message: String
  )

  object CommandStatus {
    val OK = CommandStatus(0, "")
  }

  sealed abstract class CommandCompletion extends Product with Serializable

  object CommandCompletion {

    final case class Success(commandId: CommandId) extends CommandCompletion

    final case class Checkpoint(recordTime: Instant) extends CommandCompletion

    final case class Failure(commandId: CommandId, commandStatus: CommandStatus)
        extends CommandCompletion {
      require(
        commandStatus.code != 0,
        s"Attempted to create Failure for command $commandId with '0' (success) internal status. Message: ${commandStatus.message}.")
    }

  }

  final case class RecordField(label: Option[Label], value: Value)

  sealed trait PartyTag

  type Party = String @@ PartyTag
  val Party: Tag.TagOf[PartyTag] = Tag.of[PartyTag]

  sealed trait LabelTag

  type Label = String @@ LabelTag
  val Label: Tag.TagOf[LabelTag] = Tag.of[LabelTag]

  sealed trait VariantConstructorTag

  type VariantConstructor = String @@ VariantConstructorTag
  val VariantConstructor: Tag.TagOf[VariantConstructorTag] = Tag.of[VariantConstructorTag]

  sealed trait AbsoluteOffsetTag

  type AbsoluteOffset = String @@ AbsoluteOffsetTag
  val AbsoluteOffset: Tag.TagOf[AbsoluteOffsetTag] = Tag.of[AbsoluteOffsetTag]

  sealed trait WorkflowIdTag

  type WorkflowId = String @@ WorkflowIdTag
  val WorkflowId: Tag.TagOf[WorkflowIdTag] = Tag.of[WorkflowIdTag]

  sealed trait CommandIdTag

  type CommandId = String @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  sealed trait TransactionIdTag

  type TransactionId = String @@ TransactionIdTag
  val TransactionId: Tag.TagOf[TransactionIdTag] = Tag.of[TransactionIdTag]

  sealed trait ChoiceTag

  type Choice = String @@ ChoiceTag
  val Choice: Tag.TagOf[ChoiceTag] = Tag.of[ChoiceTag]

  sealed trait ContractIdTag

  type ContractId = String @@ ContractIdTag
  val ContractId: Tag.TagOf[ContractIdTag] = Tag.of[ContractIdTag]

  sealed trait EventIdTag

  type EventId = String @@ EventIdTag
  val EventId: Tag.TagOf[EventIdTag] = Tag.of[EventIdTag]

  sealed trait PackageIdTag

  type PackageId = String @@ PackageIdTag
  val PackageId: Tag.TagOf[PackageIdTag] = Tag.of[PackageIdTag]

  sealed trait LedgerIdTag

  type LedgerId = String @@ LedgerIdTag
  val LedgerId: Tag.TagOf[LedgerIdTag] = Tag.of[LedgerIdTag]

  sealed trait ApplicationIdTag

  type ApplicationId = String @@ ApplicationIdTag
  val ApplicationId: Tag.TagOf[ApplicationIdTag] = Tag.of[ApplicationIdTag]

  case class Commands(
      ledgerId: LedgerId,
      workflowId: Option[WorkflowId],
      applicationId: ApplicationId,
      commandId: CommandId,
      submitter: Party,
      ledgerEffectiveTime: Instant,
      maximumRecordTime: Instant,
      commands: immutable.Seq[Command])

  sealed trait Command extends Product with Serializable

  final case class CreateCommand(templateId: Identifier, record: RecordValue) extends Command
  final case class ExerciseCommand(
      templateId: Identifier,
      contractId: ContractId,
      choice: Choice,
      choiceArgument: Value)
      extends Command

}
