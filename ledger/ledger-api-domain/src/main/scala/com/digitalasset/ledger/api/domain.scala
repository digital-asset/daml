// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api

import java.time.Instant

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain.Event.{CreateOrArchiveEvent, CreateOrExerciseEvent}
import scalaz.{@@, Tag}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.daml.lf.command.{Commands => LfCommands}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ValueRecord}

import scala.collection.{breakOut, immutable}

object domain {

  final case class TransactionFilter(filtersByParty: immutable.Map[Ref.Party, Filters])

  object TransactionFilter {

    /** These parties subscribe for all templates */
    def allForParties(parties: Set[Ref.Party]) =
      TransactionFilter(parties.map(_ -> Filters.noFilter)(breakOut))
  }

  final case class Filters(inclusive: Option[InclusiveFilters]) {
    def containsTemplateId(identifier: Ref.Identifier): Boolean =
      inclusive.fold(true)(_.templateIds.contains(identifier))
  }

  object Filters {
    val noFilter = Filters(None)

    def apply(inclusive: InclusiveFilters) = new Filters(Some(inclusive))
  }

  final case class InclusiveFilters(templateIds: immutable.Set[Ref.Identifier])

  sealed abstract class LedgerOffset extends Product with Serializable

  object LedgerOffset {

    final case class Absolute(value: Ref.LedgerString) extends LedgerOffset

    case object LedgerBegin extends LedgerOffset

    case object LedgerEnd extends LedgerOffset

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
        createArguments: ValueRecord[AbsoluteContractId],
        witnessParties: immutable.Set[Ref.Party],
        agreementText: String)
        extends Event
        with CreateOrExerciseEvent
        with CreateOrArchiveEvent

    final case class ArchivedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Ref.Identifier,
        witnessParties: immutable.Set[Ref.Party])
        extends Event
        with CreateOrArchiveEvent

    final case class ExercisedEvent(
        eventId: EventId,
        contractId: ContractId,
        templateId: Ref.Identifier,
        contractCreatingEventId: EventId,
        choice: Ref.ChoiceName,
        choiceArgument: Value,
        actingParties: immutable.Set[Ref.Party],
        consuming: Boolean,
        override val children: List[EventId],
        witnessParties: immutable.Set[Ref.Party],
        exerciseResult: Option[Value])
        extends Event
        with CreateOrExerciseEvent

  }

  sealed abstract class TransactionBase {

    def transactionId: TransactionId

    def commandId: Option[CommandId]

    def workflowId: Option[WorkflowId]

    def effectiveAt: Instant

    def offset: LedgerOffset.Absolute

    def traceContext: Option[TraceContext]
  }

  final case class TransactionTree(
      transactionId: TransactionId,
      commandId: Option[CommandId],
      workflowId: Option[WorkflowId],
      effectiveAt: Instant,
      offset: LedgerOffset.Absolute,
      eventsById: immutable.Map[EventId, CreateOrExerciseEvent],
      rootEventIds: immutable.Seq[EventId],
      traceContext: Option[TraceContext])
      extends TransactionBase

  final case class Transaction(
      transactionId: TransactionId,
      commandId: Option[CommandId],
      workflowId: Option[WorkflowId],
      effectiveAt: Instant,
      events: immutable.Seq[CreateOrArchiveEvent],
      offset: LedgerOffset.Absolute,
      traceContext: Option[TraceContext])
      extends TransactionBase

  type Value = Lf[Lf.AbsoluteContractId]

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

  sealed trait LabelTag

  type Label = String @@ LabelTag
  val Label: Tag.TagOf[LabelTag] = Tag.of[LabelTag]

  sealed trait VariantConstructorTag

  type VariantConstructor = String @@ VariantConstructorTag
  val VariantConstructor: Tag.TagOf[VariantConstructorTag] = Tag.of[VariantConstructorTag]

  sealed trait WorkflowIdTag

  type WorkflowId = Ref.LedgerString @@ WorkflowIdTag
  val WorkflowId: Tag.TagOf[WorkflowIdTag] = Tag.of[WorkflowIdTag]

  sealed trait CommandIdTag

  type CommandId = Ref.LedgerString @@ CommandIdTag
  val CommandId: Tag.TagOf[CommandIdTag] = Tag.of[CommandIdTag]

  sealed trait TransactionIdTag

  type TransactionId = Ref.TransactionIdString @@ TransactionIdTag
  val TransactionId: Tag.TagOf[TransactionIdTag] = Tag.of[TransactionIdTag]

  sealed trait ContractIdTag

  type ContractId = Ref.ContractIdString @@ ContractIdTag
  val ContractId: Tag.TagOf[ContractIdTag] = Tag.of[ContractIdTag]

  sealed trait EventIdTag

  type EventId = Ref.LedgerString @@ EventIdTag
  val EventId: Tag.TagOf[EventIdTag] = Tag.of[EventIdTag]

  sealed trait LedgerIdTag

  type LedgerId = String @@ LedgerIdTag
  val LedgerId: Tag.TagOf[LedgerIdTag] = Tag.of[LedgerIdTag]

  sealed trait ApplicationIdTag

  type ApplicationId = Ref.LedgerString @@ ApplicationIdTag
  val ApplicationId: Tag.TagOf[ApplicationIdTag] = Tag.of[ApplicationIdTag]

  sealed trait AbsoluteNodeIdTag

  case class Commands(
      ledgerId: LedgerId,
      workflowId: Option[WorkflowId],
      applicationId: ApplicationId,
      commandId: CommandId,
      submitter: Ref.Party,
      ledgerEffectiveTime: Instant,
      maximumRecordTime: Instant,
      commands: LfCommands)

}
