// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.interactive.interactive_submission_service.ExecuteSubmissionRequest
import com.daml.ledger.api.v2.reassignment.AssignedEvent
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsContractEntry
import com.digitalasset.canton.http.json.v2.JsSchema.JsReassignmentEvent.JsReassignmentEvent
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsEvent,
  JsInterfaceView,
  JsReassignment,
  JsReassignmentEvent,
  JsTransaction,
  JsTransactionTree,
  JsTreeEvent,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.daml.lf.data.Ref
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import ujson.StringRenderer
import ujson.circe.CirceJson

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait ConversionErrorSupport {

  /** Invalid argument on input.
    */
  protected def invalidArgument(actual: Any, expectedType: String) =
    throw new IllegalArgumentException(
      s"Expected $expectedType, got $actual"
    )

  /** Denotes unexpected/invalid value to convert. Usually Empty elements from gRPC (that are not
    * allowed to be empty in a normal situation).
    */
  def illegalValue(value: String) = throw new IllegalStateException(
    s"Value $value was not expected here"
  )
  def jsFail(err: String): Nothing = throw new IllegalArgumentException(
    err
  )
}

trait ProtocolConverter[LAPI, JS] extends ConversionErrorSupport {

  def fromJson(jsObj: JS)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[LAPI]

  def toJson(lapiObj: LAPI)(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Future[JS]

}

class ProtocolConverters(schemaProcessors: SchemaProcessors)(implicit
    val executionContext: ExecutionContext
) {

  implicit val optDeduplicationPeriodTransformer: Transformer[Option[
    lapi.commands.Commands.DeduplicationPeriod
  ], lapi.commands.Commands.DeduplicationPeriod] =
    _.getOrElse(lapi.commands.Commands.DeduplicationPeriod.Empty)

  implicit val optStringTransformer: Transformer[Option[String], String] =
    _.getOrElse("")

  implicit val optIdentifierTransformer
      : Transformer[Option[lapi.value.Identifier], lapi.value.Identifier] =
    _.getOrElse(lapi.value.Identifier.defaultInstance)

  implicit val optTimestampTransformer: Transformer[Option[
    com.google.protobuf.timestamp.Timestamp
  ], com.google.protobuf.timestamp.Timestamp] =
    _.getOrElse(com.google.protobuf.timestamp.Timestamp.defaultInstance)

  implicit def fromCirce(js: io.circe.Json): ujson.Value =
    ujson.read(CirceJson.transform(js, StringRenderer()).toString)

  implicit def toCirce(js: ujson.Value): io.circe.Json = CirceJson(js)

  object Command extends ProtocolConverter[lapi.commands.Command.Command, JsCommand.Command] {

    override def fromJson(jsObj: JsCommand.Command)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.commands.Command.Command] = jsObj match {
      case JsCommand.CreateCommand(template_id, create_arguments) =>
        for {
          protoCreateArgsRecord <-
            schemaProcessors
              .contractArgFromJsonToProto(
                template = template_id,
                jsonArgsValue = create_arguments,
              )
        } yield lapi.commands.Command.Command.Create(
          lapi.commands.CreateCommand(
            templateId = Some(template_id),
            createArguments = Some(protoCreateArgsRecord.getRecord),
          )
        )
      case JsCommand.ExerciseCommand(template_id, contract_id, choice, choice_argument) =>
        val lfChoiceName = Ref.ChoiceName.assertFromString(choice)
        for {
          choiceArgs <-
            schemaProcessors.choiceArgsFromJsonToProto(
              template = template_id,
              choiceName = lfChoiceName,
              jsonArgsValue = choice_argument,
            )
        } yield lapi.commands.Command.Command.Exercise(
          lapi.commands.ExerciseCommand(
            templateId = Some(template_id),
            contractId = contract_id,
            choiceArgument = Some(choiceArgs),
            choice = choice,
          )
        )

      case cmd: JsCommand.ExerciseByKeyCommand =>
        for {
          choiceArgs <-
            schemaProcessors.choiceArgsFromJsonToProto(
              template = cmd.templateId,
              choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
              jsonArgsValue = cmd.choiceArgument,
            )
          contractKey <-
            schemaProcessors.contractArgFromJsonToProto(
              template = cmd.templateId,
              jsonArgsValue = cmd.contractKey,
            )
        } yield lapi.commands.Command.Command.ExerciseByKey(
          lapi.commands.ExerciseByKeyCommand(
            templateId = Some(cmd.templateId),
            contractKey = Some(contractKey),
            choice = cmd.choice,
            choiceArgument = Some(choiceArgs),
          )
        )
      case cmd: JsCommand.CreateAndExerciseCommand =>
        for {
          createArgs <-
            schemaProcessors
              .contractArgFromJsonToProto(
                template = cmd.templateId,
                jsonArgsValue = cmd.createArguments,
              )
          choiceArgs <-
            schemaProcessors.choiceArgsFromJsonToProto(
              template = cmd.templateId,
              choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
              jsonArgsValue = cmd.choiceArgument,
            )
        } yield lapi.commands.Command.Command.CreateAndExercise(
          lapi.commands.CreateAndExerciseCommand(
            templateId = Some(cmd.templateId),
            createArguments = Some(createArgs.getRecord),
            choice = cmd.choice,
            choiceArgument = Some(choiceArgs),
          )
        )
    }

    override def toJson(lapiObj: lapi.commands.Command.Command)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsCommand.Command] = lapiObj match {
      case lapi.commands.Command.Command.Empty =>
        illegalValue(lapi.commands.Command.Command.Empty.toString())
      case lapi.commands.Command.Command.Create(createCommand) =>
        for {
          contractArgs <- schemaProcessors.contractArgFromProtoToJson(
            createCommand.getTemplateId,
            createCommand.getCreateArguments,
          )
        } yield JsCommand.CreateCommand(
          createCommand.getTemplateId,
          contractArgs,
        )

      case lapi.commands.Command.Command.Exercise(exerciseCommand) =>
        for {
          choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
            template = exerciseCommand.getTemplateId,
            choiceName = Ref.ChoiceName.assertFromString(exerciseCommand.choice),
            protoArgs = exerciseCommand.getChoiceArgument,
          )
        } yield JsCommand.ExerciseCommand(
          exerciseCommand.getTemplateId,
          exerciseCommand.contractId,
          exerciseCommand.choice,
          choiceArgs,
        )

      case lapi.commands.Command.Command.CreateAndExercise(cmd) =>
        for {
          createArgs <- schemaProcessors.contractArgFromProtoToJson(
            template = cmd.getTemplateId,
            protoArgs = cmd.getCreateArguments,
          )
          choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
            template = cmd.getTemplateId,
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            protoArgs = cmd.getChoiceArgument,
          )
        } yield JsCommand.CreateAndExerciseCommand(
          templateId = cmd.getTemplateId,
          createArguments = createArgs,
          choice = cmd.choice,
          choiceArgument = choiceArgs,
        )
      case lapi.commands.Command.Command.ExerciseByKey(cmd) =>
        for {
          contractKey <- schemaProcessors.keyArgFromProtoToJson(
            template = cmd.getTemplateId,
            protoArgs = cmd.getContractKey,
          )
          choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
            template = cmd.getTemplateId,
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            protoArgs = cmd.getChoiceArgument,
          )
        } yield JsCommand.ExerciseByKeyCommand(
          templateId = cmd.getTemplateId,
          contractKey = contractKey,
          choice = cmd.choice,
          choiceArgument = choiceArgs,
        )
    }
  }
  object SeqCommands
      extends ProtocolConverter[Seq[lapi.commands.Command.Command], Seq[JsCommand.Command]] {
    def toJson(commands: Seq[lapi.commands.Command.Command])(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[Seq[JsCommand.Command]] =
      Future.sequence(commands.map(Command.toJson(_)))

    def fromJson(commands: Seq[JsCommand.Command])(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[Seq[lapi.commands.Command.Command]] =
      Future.sequence(commands.map(Command.fromJson(_)))
  }

  object Commands extends ProtocolConverter[lapi.commands.Commands, JsCommands] {

    def fromJson(jsCommands: JsCommands)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.commands.Commands] =
      for {
        commands <- SeqCommands.fromJson(jsCommands.commands)
        prefetchKeys <- jsCommands.prefetchContractKeys
          .map(PrefetchContractKey.fromJson(_))
          .sequence
      } yield {
        jsCommands
          .into[lapi.commands.Commands]
          .withFieldConst(_.commands, commands.map(lapi.commands.Command(_)))
          .withFieldConst(_.prefetchContractKeys, prefetchKeys)
          .transform

      }

    def toJson(
        lapiCommands: lapi.commands.Commands
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsCommands] =
      for {
        commands <- SeqCommands.toJson(lapiCommands.commands.map(_.command))
        prefetchContractKeys <- lapiCommands.prefetchContractKeys
          .map(PrefetchContractKey.toJson(_))
          .sequence
      } yield lapiCommands
        .into[JsCommands]
        .withFieldConst(_.commands, commands)
        .withFieldConst(_.prefetchContractKeys, prefetchContractKeys)
        .transform

  }

  object InterfaceView extends ProtocolConverter[lapi.event.InterfaceView, JsInterfaceView] {

    def fromJson(
        iview: JsInterfaceView
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.event.InterfaceView] = for {

      record <- iview.viewValue
        .map { viewValue =>
          schemaProcessors
            .contractArgFromJsonToProto(
              iview.interfaceId,
              viewValue,
            )
            .map(_.getRecord)
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))
    } yield iview
      .into[lapi.event.InterfaceView]
      .withFieldConst(_.viewValue, record)
      .transform

    def toJson(
        obj: lapi.event.InterfaceView
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsInterfaceView] =
      for {
        record <- schemaProcessors.contractArgFromProtoToJson(
          obj.getInterfaceId,
          obj.getViewValue,
        )
      } yield JsInterfaceView(
        interfaceId = obj.getInterfaceId,
        viewStatus = obj.getViewStatus,
        viewValue = obj.viewValue.map(_ => record),
      )
  }

  object Event extends ProtocolConverter[lapi.event.Event.Event, JsEvent.Event] {
    def toJson(event: lapi.event.Event.Event)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsEvent.Event] =
      event match {
        case lapi.event.Event.Event.Empty => illegalValue(lapi.event.Event.Event.Empty.toString())
        case lapi.event.Event.Event.Created(value) =>
          CreatedEvent.toJson(value)
        case lapi.event.Event.Event.Archived(value) =>
          Future(ArchivedEvent.toJson(value))
        case lapi.event.Event.Event.Exercised(value) =>
          ExercisedEvent.toJson(value)
      }

    def fromJson(event: JsEvent.Event)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.event.Event.Event] = event match {
      case createdEvent: JsEvent.CreatedEvent =>
        CreatedEvent.fromJson(createdEvent).map(lapi.event.Event.Event.Created(_))
      case archivedEvent: JsEvent.ArchivedEvent =>
        Future.successful(lapi.event.Event.Event.Archived(ArchivedEvent.fromJson(archivedEvent)))
      case exercisedEvent: JsEvent.ExercisedEvent =>
        ExercisedEvent.fromJson(exercisedEvent).map(lapi.event.Event.Event.Exercised(_))
    }
  }

  object Transaction extends ProtocolConverter[lapi.transaction.Transaction, JsTransaction] {

    def toJson(v: lapi.transaction.Transaction)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsTransaction] =
      for {
        events <- v.events.map(e => Event.toJson(e.event)).sequence
      } yield {
        v.into[JsTransaction]
          .withFieldConst(_.events, events)
          .transform
      }

    def fromJson(v: JsTransaction)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.transaction.Transaction] = for {
      events <- v.events.map(Event.fromJson(_)).sequence
    } yield v
      .into[lapi.transaction.Transaction]
      .withFieldConst(_.events, events.map(lapi.event.Event(_)))
      .transform
  }

  object TransactionTreeEvent
      extends ProtocolConverter[lapi.transaction.TreeEvent.Kind, JsTreeEvent.TreeEvent] {

    override def fromJson(
        jsObj: JsTreeEvent.TreeEvent
    )(implicit errorLoggingContext: ErrorLoggingContext): Future[TreeEvent.Kind] =
      jsObj match {
        case JsTreeEvent.CreatedTreeEvent(created) =>
          CreatedEvent
            .fromJson(created)
            .map(ev => lapi.transaction.TreeEvent.Kind.Created(ev))

        case JsTreeEvent.ExercisedTreeEvent(exercised) =>
          ExercisedEvent
            .fromJson(exercised)
            .map(ev => lapi.transaction.TreeEvent.Kind.Exercised(ev))
      }

    override def toJson(lapiObj: TreeEvent.Kind)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsTreeEvent.TreeEvent] = lapiObj match {
      case lapi.transaction.TreeEvent.Kind.Empty =>
        illegalValue(lapi.transaction.TreeEvent.Kind.Empty.toString())
      case lapi.transaction.TreeEvent.Kind.Created(created) =>
        CreatedEvent.toJson(created).map(JsTreeEvent.CreatedTreeEvent(_))
      case lapi.transaction.TreeEvent.Kind.Exercised(exercised) =>
        ExercisedEvent.toJson(exercised).map(JsTreeEvent.ExercisedTreeEvent(_))
    }
  }

  object TransactionTree
      extends ProtocolConverter[lapi.transaction.TransactionTree, JsTransactionTree] {
    def toJson(
        lapiTransactionTree: lapi.transaction.TransactionTree
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsTransactionTree] =
      for {
        eventsById <- lapiTransactionTree.eventsById.toSeq.map { case (k, v) =>
          TransactionTreeEvent.toJson(v.kind).map(newVal => (k, newVal))
        }.sequence

      } yield lapiTransactionTree
        .into[JsTransactionTree]
        .withFieldConst(_.eventsById, eventsById.toMap)
        .transform

    def fromJson(
        jsTransactionTree: JsTransactionTree
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.transaction.TransactionTree] =
      for {
        eventsById <- jsTransactionTree.eventsById.toSeq.map { case (k, v) =>
          TransactionTreeEvent.fromJson(v).map(newVal => (k, lapi.transaction.TreeEvent(newVal)))
        }.sequence
      } yield jsTransactionTree
        .into[lapi.transaction.TransactionTree]
        .withFieldConst(_.eventsById, eventsById.toMap)
        .transform
  }

  object SubmitAndWaitTransactionTreeResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForTransactionTreeResponse,
        JsSubmitAndWaitForTransactionTreeResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitForTransactionTreeResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsSubmitAndWaitForTransactionTreeResponse] =
      TransactionTree
        .toJson(response.getTransaction)
        .map(tree =>
          JsSubmitAndWaitForTransactionTreeResponse(
            transactionTree = tree
          )
        )

    def fromJson(
        response: JsSubmitAndWaitForTransactionTreeResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.command_service.SubmitAndWaitForTransactionTreeResponse] =
      TransactionTree
        .fromJson(response.transactionTree)
        .map(tree =>
          lapi.command_service.SubmitAndWaitForTransactionTreeResponse(
            transaction = Some(tree)
          )
        )
  }

  object SubmitAndWaitTransactionResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForTransactionResponse,
        JsSubmitAndWaitForTransactionResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitForTransactionResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsSubmitAndWaitForTransactionResponse] =
      for {
        transaction <- Transaction
          .toJson(response.getTransaction)
      } yield JsSubmitAndWaitForTransactionResponse(
        transaction = transaction
      )

    def fromJson(
        jsResponse: JsSubmitAndWaitForTransactionResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.command_service.SubmitAndWaitForTransactionResponse] =
      for {
        transaction <- Transaction
          .fromJson(jsResponse.transaction)
      } yield lapi.command_service.SubmitAndWaitForTransactionResponse(Some(transaction))
  }

  object SubmitAndWaitForReassignmentResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForReassignmentResponse,
        JsSubmitAndWaitForReassignmentResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitForReassignmentResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsSubmitAndWaitForReassignmentResponse] =
      Reassignment
        .toJson(response.getReassignment)
        .map(reassignment =>
          JsSubmitAndWaitForReassignmentResponse(
            reassignment = reassignment
          )
        )

    def fromJson(
        jsResponse: JsSubmitAndWaitForReassignmentResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.command_service.SubmitAndWaitForReassignmentResponse] = Reassignment
      .fromJson(jsResponse.reassignment)
      .map(reassignment =>
        lapi.command_service.SubmitAndWaitForReassignmentResponse(
          reassignment = Some(reassignment)
        )
      )
  }

  object SubmitAndWaitForTransactionRequest
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForTransactionRequest,
        JsSubmitAndWaitForTransactionRequest,
      ] {

    def toJson(
        request: lapi.command_service.SubmitAndWaitForTransactionRequest
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsSubmitAndWaitForTransactionRequest] =
      for {
        commands <- Commands.toJson(request.getCommands)
      } yield {
        request
          .into[JsSubmitAndWaitForTransactionRequest]
          .withFieldConst(_.commands, commands)
          .transform
      }

    def fromJson(
        jsRequest: JsSubmitAndWaitForTransactionRequest
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.command_service.SubmitAndWaitForTransactionRequest] =
      for {
        commands <- Commands.fromJson(jsRequest.commands)
      } yield jsRequest
        .into[lapi.command_service.SubmitAndWaitForTransactionRequest]
        .withFieldConst(_.commands, Some(commands))
        .transform

  }

  object GetEventsByContractIdResponse
      extends ProtocolConverter[
        lapi.event_query_service.GetEventsByContractIdResponse,
        JsGetEventsByContractIdResponse,
      ] {
    def toJson(
        response: lapi.event_query_service.GetEventsByContractIdResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetEventsByContractIdResponse] =
      for {
        createdEvents <- response.created
          .map(c => CreatedEvent.toJson(c.getCreatedEvent).map(Some(_)))
          .getOrElse(Future(None))
      } yield JsGetEventsByContractIdResponse(
        created = response.created.flatMap(c =>
          createdEvents.map(ce =>
            JsCreated(
              createdEvent = ce,
              synchronizerId = c.synchronizerId,
            )
          )
        ),
        archived = response.archived.map(a =>
          JsArchived(
            archivedEvent = ArchivedEvent.toJson(a.getArchivedEvent),
            synchronizerId = a.synchronizerId,
          )
        ),
      )

    def fromJson(
        obj: JsGetEventsByContractIdResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.event_query_service.GetEventsByContractIdResponse] = for {
      createdEvents <- obj.created
        .map(c => CreatedEvent.fromJson(c.createdEvent).map(Some(_)))
        .getOrElse(Future(None))
    } yield lapi.event_query_service.GetEventsByContractIdResponse(
      created = obj.created.flatMap((c: JsCreated) =>
        createdEvents.map(ce => lapi.event_query_service.Created(Some(ce), c.synchronizerId))
      ),
      archived = obj.archived.map(arch =>
        lapi.event_query_service.Archived(
          archivedEvent = Some(ArchivedEvent.fromJson(arch.archivedEvent)),
          synchronizerId = arch.synchronizerId,
        )
      ),
    )
  }

  object ArchivedEvent {
    def toJson(e: lapi.event.ArchivedEvent): JsEvent.ArchivedEvent =
      e.into[JsEvent.ArchivedEvent].transform

    def fromJson(ev: JsEvent.ArchivedEvent): lapi.event.ArchivedEvent =
      ev.into[lapi.event.ArchivedEvent].transform
  }

  object CreatedEvent extends ProtocolConverter[lapi.event.CreatedEvent, JsEvent.CreatedEvent] {
    def toJson(created: lapi.event.CreatedEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsEvent.CreatedEvent] =
      for {
        contractKey <- created.contractKey
          .map(ck =>
            schemaProcessors
              .keyArgFromProtoToJson(
                created.getTemplateId,
                ck,
              )
          )
          .sequence
        createdArgs <- created.createArguments
          .map(ca =>
            schemaProcessors
              .contractArgFromProtoToJson(
                created.getTemplateId,
                ca,
              )
          )
          .sequence
        interfaceViews <- Future.sequence(created.interfaceViews.map(InterfaceView.toJson))
      } yield created
        .into[JsEvent.CreatedEvent]
        .withFieldConst(_.interfaceViews, interfaceViews)
        .withFieldConst(_.contractKey, contractKey.map(toCirce(_)))
        .withFieldConst(_.createArgument, createdArgs.map(toCirce(_)))
        .transform

    def fromJson(createdEvent: JsEvent.CreatedEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.event.CreatedEvent] = {
      val templateId = createdEvent.templateId
      for {
        contractKey <- createdEvent.contractKey
          .map(key =>
            schemaProcessors
              .keyArgFromJsonToProto(templateId, key)
          )
          .sequence

        createArgs <- createdEvent.createArgument
          .map(args =>
            schemaProcessors
              .contractArgFromJsonToProto(templateId, args)
          )
          .sequence

        interfaceViews <- Future.sequence(createdEvent.interfaceViews.map(InterfaceView.fromJson))
      } yield createdEvent
        .into[lapi.event.CreatedEvent]
        .withFieldConst(_.createArguments, createArgs.map(_.getRecord))
        .withFieldConst(_.contractKey, contractKey)
        .withFieldConst(_.interfaceViews, interfaceViews)
        .transform
    }

  }

  object ExercisedEvent
      extends ProtocolConverter[lapi.event.ExercisedEvent, JsEvent.ExercisedEvent] {
    def toJson(exercised: lapi.event.ExercisedEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsEvent.ExercisedEvent] =
      for {
        choiceArgs <-
          schemaProcessors
            .choiceArgsFromProtoToJson(
              exercised.interfaceId.getOrElse(exercised.getTemplateId),
              Ref.ChoiceName.assertFromString(exercised.choice),
              exercised.getChoiceArgument,
            )
        exerciseResult <-
          schemaProcessors
            .exerciseResultFromProtoToJson(
              exercised.interfaceId.getOrElse(exercised.getTemplateId),
              Ref.ChoiceName.assertFromString(exercised.choice),
              exercised.getExerciseResult,
            )
      } yield {
        exercised
          .into[JsEvent.ExercisedEvent]
          .withFieldConst(_.exerciseResult, toCirce(exerciseResult))
          .withFieldConst(_.choiceArgument, toCirce(choiceArgs))
          .transform
      }

    def fromJson(exercisedEvent: JsEvent.ExercisedEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.event.ExercisedEvent] =
      for {
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            exercisedEvent.interfaceId.getOrElse(exercisedEvent.templateId),
            Ref.ChoiceName.assertFromString(exercisedEvent.choice),
            exercisedEvent.choiceArgument,
          )
        exerciseResult <-
          schemaProcessors
            .exerciseResultFromJsonToProto(
              exercisedEvent.interfaceId.getOrElse(exercisedEvent.templateId),
              Ref.ChoiceName.assertFromString(exercisedEvent.choice),
              exercisedEvent.exerciseResult,
            )
      } yield exercisedEvent
        .into[lapi.event.ExercisedEvent]
        .withFieldConst(_.exerciseResult, exerciseResult)
        .withFieldConst(_.choiceArgument, Some(choiceArgs))
        .transform
  }

  object AssignedEvent
      extends ProtocolConverter[
        lapi.reassignment.AssignedEvent,
        JsAssignedEvent,
      ] {

    def toJson(v: lapi.reassignment.AssignedEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsAssignedEvent] =
      for {
        createdEvent <- CreatedEvent.toJson(v.getCreatedEvent)
      } yield v
        .into[JsAssignedEvent]
        .withFieldConst(_.createdEvent, createdEvent)
        .transform

    override def fromJson(
        jsObj: JsAssignedEvent
    )(implicit errorLoggingContext: ErrorLoggingContext): Future[AssignedEvent] =
      for {
        createdEvent <- CreatedEvent.fromJson(jsObj.createdEvent)
      } yield jsObj
        .into[lapi.reassignment.AssignedEvent]
        .withFieldConst(_.createdEvent, Some(createdEvent))
        .transform
  }

  object ContractEntry
      extends ProtocolConverter[
        lapi.state_service.GetActiveContractsResponse.ContractEntry,
        JsContractEntry,
      ] {
    def toJson(
        v: lapi.state_service.GetActiveContractsResponse.ContractEntry
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsContractEntry] =
      v match {
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty =>
          Future(JsContractEntry.JsEmpty)
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(value) =>
          for {
            createdEvent <- CreatedEvent.toJson(value.getCreatedEvent)
          } yield value
            .into[JsContractEntry.JsActiveContract]
            .withFieldConst(_.createdEvent, createdEvent)
            .transform

        case lapi.state_service.GetActiveContractsResponse.ContractEntry
              .IncompleteUnassigned(value) =>
          for {
            createdEvent <- CreatedEvent.toJson(value.getCreatedEvent)
          } yield value
            .into[JsContractEntry.JsIncompleteUnassigned]
            .withFieldConst(_.createdEvent, createdEvent)
            .withFieldComputed(_.unassignedEvent, _.getUnassignedEvent)
            .transform

        case lapi.state_service.GetActiveContractsResponse.ContractEntry
              .IncompleteAssigned(value) =>
          AssignedEvent
            .toJson(value.getAssignedEvent)
            .map(ae =>
              JsContractEntry.JsIncompleteAssigned(
                ae
              )
            )
      }

    def fromJson(
        jsContractEntry: JsContractEntry
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.state_service.GetActiveContractsResponse.ContractEntry] = jsContractEntry match {
      case JsContractEntry.JsEmpty =>
        Future(lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty)
      case JsContractEntry.JsIncompleteAssigned(assigned_event) =>
        for {
          createdEvent <- CreatedEvent.fromJson(assigned_event.createdEvent)
        } yield lapi.state_service.GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
          lapi.state_service.IncompleteAssigned(
            Some(
              assigned_event
                .into[lapi.reassignment.AssignedEvent]
                .withFieldConst(_.createdEvent, Some(createdEvent))
                .transform
            )
          )
        )

      case JsContractEntry.JsIncompleteUnassigned(created_event, unassigned_event) =>
        for {
          created <- CreatedEvent.fromJson(created_event)
        } yield lapi.state_service.GetActiveContractsResponse.ContractEntry.IncompleteUnassigned(
          lapi.state_service.IncompleteUnassigned(
            createdEvent = Some(created),
            unassignedEvent = Some(unassigned_event),
          )
        )

      case JsContractEntry.JsActiveContract(created_event, synchronizer_id, reassignment_counter) =>
        CreatedEvent
          .fromJson(created_event)
          .map(ce =>
            lapi.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(
              new lapi.state_service.ActiveContract(
                createdEvent = Some(ce),
                synchronizerId = synchronizer_id,
                reassignmentCounter = reassignment_counter,
              )
            )
          )
    }

  }

  object GetActiveContractsResponse
      extends ProtocolConverter[
        lapi.state_service.GetActiveContractsResponse,
        JsGetActiveContractsResponse,
      ] {
    def toJson(v: lapi.state_service.GetActiveContractsResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetActiveContractsResponse] =
      for {
        contractEntry <- ContractEntry.toJson(v.contractEntry)
      } yield v
        .into[JsGetActiveContractsResponse]
        .withFieldConst(_.contractEntry, contractEntry)
        .transform

    def fromJson(
        v: JsGetActiveContractsResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.state_service.GetActiveContractsResponse] =
      ContractEntry
        .fromJson(v.contractEntry)
        .map(ce =>
          lapi.state_service.GetActiveContractsResponse(
            workflowId = v.workflowId,
            contractEntry = ce,
          )
        )
  }

  object ReassignmentEvent
      extends ProtocolConverter[lapi.reassignment.ReassignmentEvent.Event, JsReassignmentEvent] {
    def toJson(v: lapi.reassignment.ReassignmentEvent.Event)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsReassignmentEvent] =
      v match {
        case lapi.reassignment.ReassignmentEvent.Event.Empty =>
          illegalValue(lapi.reassignment.ReassignmentEvent.Event.Empty.toString())
        case lapi.reassignment.ReassignmentEvent.Event.Unassigned(value) =>
          Future(JsReassignmentEvent.JsUnassignedEvent(value))
        case lapi.reassignment.ReassignmentEvent.Event.Assigned(value) =>
          for {
            createdEvent <- CreatedEvent.toJson(value.getCreatedEvent)
          } yield value
            .into[JsReassignmentEvent.JsAssignmentEvent]
            .withFieldConst(_.createdEvent, createdEvent)
            .transform
      }

    def fromJson(jsObj: JsReassignmentEvent)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.reassignment.ReassignmentEvent.Event] =
      jsObj match {
        case event: JsReassignmentEvent.JsAssignmentEvent =>
          for {
            createdEvent <- CreatedEvent.fromJson(event.createdEvent)
          } yield lapi.reassignment.ReassignmentEvent.Event.Assigned(value =
            event
              .into[lapi.reassignment.AssignedEvent]
              .withFieldConst(_.createdEvent, Some(createdEvent))
              .transform
          )

        case JsReassignmentEvent.JsUnassignedEvent(value) =>
          Future.successful(lapi.reassignment.ReassignmentEvent.Event.Unassigned(value))
      }

  }
  object Reassignment extends ProtocolConverter[lapi.reassignment.Reassignment, JsReassignment] {
    def toJson(v: lapi.reassignment.Reassignment)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsReassignment] =
      for {
        events <- v.events
          .traverse(e => ReassignmentEvent.toJson(e.event))
      } yield {
        v.into[JsReassignment]
          .withFieldConst(_.events, events)
          .transform
      }

    def fromJson(value: JsReassignment)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.reassignment.Reassignment] =
      for {
        events <- value.events
          .traverse(e => ReassignmentEvent.fromJson(e))
      } yield value
        .into[lapi.reassignment.Reassignment]
        .withFieldConst(_.events, events.map(lapi.reassignment.ReassignmentEvent(_)))
        .transform
  }

  object GetUpdatesResponse
      extends ProtocolConverter[lapi.update_service.GetUpdatesResponse, JsGetUpdatesResponse] {
    def toJson(obj: lapi.update_service.GetUpdatesResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetUpdatesResponse] =
      ((obj.update match {
        case lapi.update_service.GetUpdatesResponse.Update.Empty =>
          illegalValue(lapi.update_service.GetUpdatesResponse.Update.Empty.toString())
        case lapi.update_service.GetUpdatesResponse.Update.Transaction(value) =>
          Transaction.toJson(value).map(JsUpdate.Transaction.apply)
        case lapi.update_service.GetUpdatesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdate.Reassignment.apply)
        case lapi.update_service.GetUpdatesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdate.OffsetCheckpoint(value))
        case lapi.update_service.GetUpdatesResponse.Update.TopologyTransaction(value) =>
          Future(JsUpdate.TopologyTransaction(value))
      }): Future[JsUpdate.Update]).map(update => JsGetUpdatesResponse(update))

    def fromJson(obj: JsGetUpdatesResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.update_service.GetUpdatesResponse] =
      (obj.update match {
        case JsUpdate.OffsetCheckpoint(value) =>
          Future.successful(
            lapi.update_service.GetUpdatesResponse.Update.OffsetCheckpoint(
              value
            )
          )
        case JsUpdate.Reassignment(value) =>
          Reassignment
            .fromJson(value)
            .map(
              lapi.update_service.GetUpdatesResponse.Update.Reassignment.apply
            )
        case JsUpdate.Transaction(value) =>
          Transaction.fromJson(value).map { tr =>
            lapi.update_service.GetUpdatesResponse.Update.Transaction(tr)
          }
        case JsUpdate.TopologyTransaction(value) =>
          Future.successful(
            lapi.update_service.GetUpdatesResponse.Update.TopologyTransaction(value)
          )
      }).map(lapi.update_service.GetUpdatesResponse(_))
  }

  object GetUpdateResponse
      extends ProtocolConverter[lapi.update_service.GetUpdateResponse, JsGetUpdateResponse] {
    def toJson(obj: lapi.update_service.GetUpdateResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetUpdateResponse] =
      ((obj.update match {
        case lapi.update_service.GetUpdateResponse.Update.Empty =>
          illegalValue(lapi.update_service.GetUpdateResponse.Update.Empty.toString())
        case lapi.update_service.GetUpdateResponse.Update.Transaction(value) =>
          Transaction.toJson(value).map(JsUpdate.Transaction.apply)
        case lapi.update_service.GetUpdateResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdate.Reassignment.apply)
        case lapi.update_service.GetUpdateResponse.Update.TopologyTransaction(value) =>
          Future.successful(JsUpdate.TopologyTransaction(value))
      }): Future[JsUpdate.Update]).map(update => JsGetUpdateResponse(update))

    def fromJson(obj: JsGetUpdateResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.update_service.GetUpdateResponse] =
      (obj.update match {
        case JsUpdate.Reassignment(value) =>
          Reassignment
            .fromJson(value)
            .map(
              lapi.update_service.GetUpdateResponse.Update.Reassignment.apply
            )
        case JsUpdate.Transaction(value) =>
          Transaction.fromJson(value).map { tr =>
            lapi.update_service.GetUpdateResponse.Update.Transaction(tr)
          }
        case JsUpdate.TopologyTransaction(value) =>
          Future.successful(lapi.update_service.GetUpdateResponse.Update.TopologyTransaction(value))
        case JsUpdate.OffsetCheckpoint(_) =>
          Future.failed(
            new RuntimeException(
              "The unexpected happened! A pointwise query should not have returned an OffsetCheckpoint update."
            )
          )
      }).map(lapi.update_service.GetUpdateResponse(_))
  }

  object GetUpdateTreesResponse
      extends ProtocolConverter[
        lapi.update_service.GetUpdateTreesResponse,
        JsGetUpdateTreesResponse,
      ] {
    def toJson(
        value: lapi.update_service.GetUpdateTreesResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetUpdateTreesResponse] =
      ((value.update match {
        case lapi.update_service.GetUpdateTreesResponse.Update.Empty =>
          illegalValue(lapi.update_service.GetUpdateTreesResponse.Update.Empty.toString())
        case lapi.update_service.GetUpdateTreesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdateTree.OffsetCheckpoint(value))
        case lapi.update_service.GetUpdateTreesResponse.Update.TransactionTree(value) =>
          TransactionTree.toJson(value).map(JsUpdateTree.TransactionTree.apply)
        case lapi.update_service.GetUpdateTreesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdateTree.Reassignment.apply)
      }): Future[JsUpdateTree.Update]).map(update => JsGetUpdateTreesResponse(update))

    def fromJson(
        jsObj: JsGetUpdateTreesResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.update_service.GetUpdateTreesResponse] =
      (jsObj.update match {
        case JsUpdateTree.OffsetCheckpoint(value) =>
          Future.successful(
            lapi.update_service.GetUpdateTreesResponse.Update.OffsetCheckpoint(value)
          )
        case JsUpdateTree.Reassignment(value) =>
          Reassignment
            .fromJson(value)
            .map(lapi.update_service.GetUpdateTreesResponse.Update.Reassignment.apply)
        case JsUpdateTree.TransactionTree(value) =>
          TransactionTree
            .fromJson(value)
            .map(lapi.update_service.GetUpdateTreesResponse.Update.TransactionTree.apply)
      }).map(lapi.update_service.GetUpdateTreesResponse(_))
  }

  object GetTransactionTreeResponse
      extends ProtocolConverter[
        lapi.update_service.GetTransactionTreeResponse,
        JsGetTransactionTreeResponse,
      ] {
    def toJson(
        obj: lapi.update_service.GetTransactionTreeResponse
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetTransactionTreeResponse] =
      TransactionTree.toJson(obj.getTransaction).map(JsGetTransactionTreeResponse.apply)

    def fromJson(treeResponse: JsGetTransactionTreeResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.update_service.GetTransactionTreeResponse] =
      TransactionTree
        .fromJson(treeResponse.transaction)
        .map(tree => lapi.update_service.GetTransactionTreeResponse(Some(tree)))

  }

  object GetTransactionResponse
      extends ProtocolConverter[
        lapi.update_service.GetTransactionResponse,
        JsGetTransactionResponse,
      ] {
    def toJson(obj: lapi.update_service.GetTransactionResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsGetTransactionResponse] =
      Transaction.toJson(obj.getTransaction).map(JsGetTransactionResponse.apply)

    def fromJson(obj: JsGetTransactionResponse)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.update_service.GetTransactionResponse] =
      Transaction
        .fromJson(obj.transaction)
        .map(tr => lapi.update_service.GetTransactionResponse(Some(tr)))
  }

  object PrepareSubmissionRequest {
    def fromJson(obj: JsPrepareSubmissionRequest)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest] = for {
      commands <- SeqCommands.fromJson(obj.commands)
      prefetchContractKeys <- obj.prefetchContractKeys.map(PrefetchContractKey.fromJson).sequence
    } yield obj
      .into[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest]
      .withFieldConst(_.commands, commands.map(lapi.commands.Command(_)))
      .withFieldConst(_.prefetchContractKeys, prefetchContractKeys)
      .transform

  }

  object PrepareSubmissionResponse {
    def toJson(
        obj: lapi.interactive.interactive_submission_service.PrepareSubmissionResponse
    ): Future[JsPrepareSubmissionResponse] =
      Future.successful(
        obj
          .into[JsPrepareSubmissionResponse]
          .withFieldConst(_.preparedTransaction, obj.preparedTransaction.map(_.toByteString))
          .transform
      )
  }

  object ExecuteSubmissionRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest,
        JsExecuteSubmissionRequest,
      ] {
    def fromJson(
        obj: JsExecuteSubmissionRequest
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest] =
      Future {
        val preparedTransaction = obj.preparedTransaction.map { proto =>
          ProtoConverter
            .protoParser(
              lapi.interactive.interactive_submission_service.PreparedTransaction.parseFrom
            )(proto)
            .getOrElse(jsFail("Cannot parse prepared_transaction"))
        }
        obj
          .into[lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest]
          .withFieldConst(_.preparedTransaction, preparedTransaction)
          .transform

      }

    override def toJson(lapi: ExecuteSubmissionRequest)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[JsExecuteSubmissionRequest] = jsFail("not supported")
  }

  object AllocatePartyRequest
      extends ProtocolConverter[
        lapi.admin.party_management_service.AllocatePartyRequest,
        js.AllocatePartyRequest,
      ] {
    def fromJson(
        obj: js.AllocatePartyRequest
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.admin.party_management_service.AllocatePartyRequest] =
      Future.successful(
        obj.into[lapi.admin.party_management_service.AllocatePartyRequest].transform
      )

    def toJson(
        obj: lapi.admin.party_management_service.AllocatePartyRequest
    )(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[js.AllocatePartyRequest] = Future.successful(
      obj.into[js.AllocatePartyRequest].transform
    )
  }

  object PrefetchContractKey
      extends ProtocolConverter[
        lapi.commands.PrefetchContractKey,
        js.PrefetchContractKey,
      ] {
    def fromJson(obj: js.PrefetchContractKey)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[lapi.commands.PrefetchContractKey] =
      for {
        contractKey <- obj.templateId
          .traverse(template => schemaProcessors.keyArgFromJsonToProto(template, obj.contractKey))
      } yield obj
        .into[lapi.commands.PrefetchContractKey]
        .withFieldConst(_.contractKey, contractKey)
        .transform

    def toJson(obj: lapi.commands.PrefetchContractKey)(implicit
        errorLoggingContext: ErrorLoggingContext
    ): Future[js.PrefetchContractKey] = for {
      contractKey <- obj.contractKey
        .flatMap(ck =>
          obj.templateId.map(template =>
            schemaProcessors.keyArgFromProtoToJson(template, ck).map(Some(_))
          )
        )
        .getOrElse(Future(None))

    } yield obj
      .into[js.PrefetchContractKey]
      .withFieldConst(_.contractKey, toCirce(contractKey.getOrElse(ujson.Null)))
      .transform
  }
}

object IdentifierConverter extends ConversionErrorSupport {
  def fromJson(jsIdentifier: String): lapi.value.Identifier =
    jsIdentifier.split(":").toSeq match {
      case Seq(packageId, moduleName, entityName) =>
        lapi.value.Identifier(
          packageId = packageId,
          moduleName = moduleName,
          entityName = entityName,
        )
      case _ => invalidArgument(jsIdentifier, "<package>:<moduleName>:<entityName>")
    }

  def toJson(lapiIdentifier: lapi.value.Identifier): String =
    s"${lapiIdentifier.packageId}:${lapiIdentifier.moduleName}:${lapiIdentifier.entityName}"
}
