// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.interactive.interactive_submission_service.{
  ExecuteSubmissionRequest,
  PrepareSubmissionRequest,
  PrepareSubmissionResponse,
  PreparedTransaction,
}
import com.daml.ledger.api.v2.reassignment.AssignedEvent
import com.digitalasset.canton.http.json.v2.IdentifierConverter.illegalValue
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
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import com.google.rpc.Code
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
      traceContext: TraceContext
  ): Future[LAPI]

  def toJson(lapiObj: LAPI)(implicit
      traceContext: TraceContext
  ): Future[JS]

}

class ProtocolConverters(
    schemaProcessors: SchemaProcessors,
    transcodePackageIdResolver: TranscodePackageIdResolver,
)(implicit
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

  object Command {

    def fromJson(jsObj: JsCommand.Command, decodingPackageId: String)(implicit
        traceContext: TraceContext
    ): Future[lapi.commands.Command.Command] = {
      implicit class WithDecodingPackageId(orig: lapi.value.Identifier) {
        def withDecodingPackageId: lapi.value.Identifier = orig.copy(packageId = decodingPackageId)
      }
      jsObj match {
        case JsCommand.CreateCommand(template_id, create_arguments) =>
          for {
            protoCreateArgsRecord <-
              schemaProcessors
                .contractArgFromJsonToProto(
                  template = template_id.withDecodingPackageId,
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
                template = template_id.withDecodingPackageId,
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
                template = cmd.templateId.withDecodingPackageId,
                choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
                jsonArgsValue = cmd.choiceArgument,
              )
            contractKey <-
              schemaProcessors.contractArgFromJsonToProto(
                template = cmd.templateId.withDecodingPackageId,
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
                  template = cmd.templateId.withDecodingPackageId,
                  jsonArgsValue = cmd.createArguments,
                )
            choiceArgs <-
              schemaProcessors.choiceArgsFromJsonToProto(
                template = cmd.templateId.withDecodingPackageId,
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
    }

    def toJson(lapiObj: lapi.commands.Command.Command, decodingPackageId: String)(implicit
        traceContext: TraceContext
    ): Future[JsCommand.Command] = {
      implicit class WithDecodingPackageId(orig: lapi.value.Identifier) {
        def withDecodingPackageId: lapi.value.Identifier = orig.copy(packageId = decodingPackageId)
      }
      lapiObj match {
        case lapi.commands.Command.Command.Empty =>
          illegalValue(lapi.commands.Command.Command.Empty.toString())
        case lapi.commands.Command.Command.Create(createCommand) =>
          for {
            contractArgs <- schemaProcessors.contractArgFromProtoToJson(
              createCommand.getTemplateId.withDecodingPackageId,
              createCommand.getCreateArguments,
            )
          } yield JsCommand.CreateCommand(
            createCommand.getTemplateId,
            contractArgs,
          )

        case lapi.commands.Command.Command.Exercise(exerciseCommand) =>
          for {
            choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
              template = exerciseCommand.getTemplateId.withDecodingPackageId,
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
              template = cmd.getTemplateId.withDecodingPackageId,
              protoArgs = cmd.getCreateArguments,
            )
            choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
              template = cmd.getTemplateId.withDecodingPackageId,
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
              template = cmd.getTemplateId.withDecodingPackageId,
              protoArgs = cmd.getContractKey,
            )
            choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
              template = cmd.getTemplateId.withDecodingPackageId,
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
  }

  object SeqCommands {
    def toJson(
        commands: Seq[
          (
              /* LAPI command */ lapi.commands.Command.Command,
              /* The package-id the argument should be decoded with */ String,
          )
        ]
    )(implicit
        traceContext: TraceContext
    ): Future[Seq[JsCommand.Command]] =
      Future.sequence(commands.map { case (cmd, decodingPackageId) =>
        Command.toJson(cmd, decodingPackageId)
      })

    def fromJson(
        commands: Seq[
          (
              /* LAPI command */ JsCommand.Command,
              /* The package-id the argument should be decoded with */ String,
          )
        ]
    )(implicit
        traceContext: TraceContext
    ): Future[Seq[lapi.commands.Command.Command]] =
      Future.sequence(commands.map { case (cmd, decodingPackageId) =>
        Command.fromJson(cmd, decodingPackageId)
      })
  }

  object Commands extends ProtocolConverter[lapi.commands.Commands, JsCommands] {

    def fromJson(jsCommands: JsCommands)(implicit
        traceContext: TraceContext
    ): Future[lapi.commands.Commands] =
      for {
        commandsWithTranscodingPackageIds <- transcodePackageIdResolver
          .resolveDecodingPackageIdsForJsonCommands(
            jsCommands = jsCommands.commands,
            actAs = jsCommands.actAs,
            packageIdSelectionPreference = jsCommands.packageIdSelectionPreference,
            synchronizerIdO = jsCommands.synchronizerId.filter(_.nonEmpty),
          )

        transcodedCommands <- SeqCommands.fromJson(commandsWithTranscodingPackageIds)

        // TODO(#27501): Select packages for prefetched contract keys
        prefetchKeys <- jsCommands.prefetchContractKeys
          .map(PrefetchContractKey.fromJson(_))
          .sequence
      } yield {
        jsCommands
          .into[lapi.commands.Commands]
          .withFieldConst(_.commands, transcodedCommands.map(lapi.commands.Command(_)))
          .withFieldConst(_.prefetchContractKeys, prefetchKeys)
          .transform
      }

    def toJson(
        lapiCommands: lapi.commands.Commands
    )(implicit
        traceContext: TraceContext
    ): Future[JsCommands] =
      for {
        commandsWithTranscodingPackageIds <- transcodePackageIdResolver
          .resolveDecodingPackageIdsForGrpcCommands(
            grpcCommands = lapiCommands.commands.map(_.command),
            actAs = lapiCommands.actAs,
            packageIdSelectionPreference = lapiCommands.packageIdSelectionPreference,
            synchronizerIdO = Option(lapiCommands.synchronizerId).filter(_.nonEmpty),
          )
        transcodedCommands <- SeqCommands.toJson(commandsWithTranscodingPackageIds)
        // TODO(#27501): Select packages for prefetched contract keys
        prefetchContractKeys <- lapiCommands.prefetchContractKeys
          .map(PrefetchContractKey.toJson(_))
          .sequence
      } yield lapiCommands
        .into[JsCommands]
        .withFieldConst(_.commands, transcodedCommands)
        .withFieldConst(_.prefetchContractKeys, prefetchContractKeys)
        .transform
  }

  object InterfaceView extends ProtocolConverter[lapi.event.InterfaceView, JsInterfaceView] {

    def fromJson(
        iview: JsInterfaceView
    )(implicit
        traceContext: TraceContext
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
        traceContext: TraceContext
    ): Future[JsInterfaceView] = {
      val viewStatus = obj.getViewStatus
      if (viewStatus.code != Code.OK.getNumber) {
        Future.successful(
          JsInterfaceView(
            interfaceId = obj.getInterfaceId,
            viewStatus = viewStatus,
            viewValue = None,
          )
        )
      } else
        for {
          record <- schemaProcessors.contractArgFromProtoToJson(
            obj.getInterfaceId,
            obj.getViewValue,
          )
        } yield JsInterfaceView(
          interfaceId = obj.getInterfaceId,
          viewStatus = viewStatus,
          viewValue = obj.viewValue.map(_ => record),
        )
    }
  }

  object Event extends ProtocolConverter[lapi.event.Event.Event, JsEvent.Event] {
    def toJson(event: lapi.event.Event.Event)(implicit
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
    ): Future[JsTransaction] =
      for {
        events <- v.events.map(e => Event.toJson(e.event)).sequence
      } yield {
        v.into[JsTransaction]
          .withFieldConst(_.events, events)
          .withFieldComputed(
            _.externalTransactionHash,
            _.externalTransactionHash
              .map(_.toByteArray)
              .map(Hash.assertFromByteArray)
              .map(_.toHexString),
          )
          .transform
      }

    def fromJson(v: JsTransaction)(implicit
        traceContext: TraceContext
    ): Future[lapi.transaction.Transaction] = for {
      events <- v.events.map(Event.fromJson(_)).sequence
    } yield v
      .into[lapi.transaction.Transaction]
      .withFieldConst(_.events, events.map(lapi.event.Event(_)))
      .withFieldComputed(
        _.externalTransactionHash,
        _.externalTransactionHash.map(ByteString.fromHex),
      )
      .transform
  }

  object TransactionTreeEventLegacy
      extends ProtocolConverter[LegacyDTOs.TreeEvent.Kind, JsTreeEvent.TreeEvent] {

    override def fromJson(
        jsObj: JsTreeEvent.TreeEvent
    )(implicit traceContext: TraceContext): Future[LegacyDTOs.TreeEvent.Kind] =
      jsObj match {
        case JsTreeEvent.CreatedTreeEvent(created) =>
          CreatedEvent
            .fromJson(created)
            .map(ev => LegacyDTOs.TreeEvent.Kind.Created(ev))

        case JsTreeEvent.ExercisedTreeEvent(exercised) =>
          ExercisedEvent
            .fromJson(exercised)
            .map(ev => LegacyDTOs.TreeEvent.Kind.Exercised(ev))
      }

    override def toJson(lapiObj: LegacyDTOs.TreeEvent.Kind)(implicit
        traceContext: TraceContext
    ): Future[JsTreeEvent.TreeEvent] = lapiObj match {
      case LegacyDTOs.TreeEvent.Kind.Empty =>
        illegalValue(LegacyDTOs.TreeEvent.Kind.Empty.toString())
      case LegacyDTOs.TreeEvent.Kind.Created(created) =>
        CreatedEvent.toJson(created).map(JsTreeEvent.CreatedTreeEvent(_))
      case LegacyDTOs.TreeEvent.Kind.Exercised(exercised) =>
        ExercisedEvent.toJson(exercised).map(JsTreeEvent.ExercisedTreeEvent(_))
    }
  }

  object TransactionTreeLegacy
      extends ProtocolConverter[LegacyDTOs.TransactionTree, JsTransactionTree] {
    def toJson(
        transactionTree: LegacyDTOs.TransactionTree
    )(implicit
        traceContext: TraceContext
    ): Future[JsTransactionTree] =
      for {
        eventsById <- transactionTree.eventsById.toSeq.map { case (k, v) =>
          TransactionTreeEventLegacy.toJson(v.kind).map(newVal => (k, newVal))
        }.sequence

      } yield transactionTree
        .into[JsTransactionTree]
        .withFieldConst(_.eventsById, eventsById.toMap)
        .transform

    def fromJson(
        jsTransactionTree: JsTransactionTree
    )(implicit
        traceContext: TraceContext
    ): Future[LegacyDTOs.TransactionTree] =
      for {
        eventsById <- jsTransactionTree.eventsById.toSeq.map { case (k, v) =>
          TransactionTreeEventLegacy
            .fromJson(v)
            .map(newVal => (k, LegacyDTOs.TreeEvent(newVal)))
        }.sequence
      } yield jsTransactionTree
        .into[LegacyDTOs.TransactionTree]
        .withFieldConst(_.eventsById, eventsById.toMap)
        .transform
  }

  object SubmitAndWaitTransactionTreeResponseLegacy
      extends ProtocolConverter[
        LegacyDTOs.SubmitAndWaitForTransactionTreeResponse,
        JsSubmitAndWaitForTransactionTreeResponse,
      ] {

    def toJson(
        response: LegacyDTOs.SubmitAndWaitForTransactionTreeResponse
    )(implicit
        traceContext: TraceContext
    ): Future[JsSubmitAndWaitForTransactionTreeResponse] =
      TransactionTreeLegacy
        .toJson(response.transaction.getOrElse(invalidArgument("empty", "non-empty transaction")))
        .map(tree =>
          JsSubmitAndWaitForTransactionTreeResponse(
            transactionTree = tree
          )
        )

    def fromJson(
        response: JsSubmitAndWaitForTransactionTreeResponse
    )(implicit
        traceContext: TraceContext
    ): Future[LegacyDTOs.SubmitAndWaitForTransactionTreeResponse] =
      TransactionTreeLegacy
        .fromJson(response.transactionTree)
        .map(tree =>
          LegacyDTOs.SubmitAndWaitForTransactionTreeResponse(
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
    ): Future[JsEvent.CreatedEvent] = {
      val representativeTemplateId =
        created.getTemplateId.copy(packageId = created.representativePackageId)

      for {
        contractKey <- created.contractKey
          .traverse(schemaProcessors.keyArgFromProtoToJson(representativeTemplateId, _))
        createdArgs <- created.createArguments.traverse(
          schemaProcessors.contractArgFromProtoToJson(representativeTemplateId, _)
        )
        interfaceViews <- Future.sequence(created.interfaceViews.map(InterfaceView.toJson))
      } yield created
        .into[JsEvent.CreatedEvent]
        .withFieldConst(_.interfaceViews, interfaceViews)
        .withFieldConst(_.contractKey, contractKey.map(toCirce(_)))
        .withFieldConst(_.createArgument, createdArgs.map(toCirce(_)))
        .transform
    }

    def fromJson(createdEvent: JsEvent.CreatedEvent)(implicit
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
    ): Future[JsAssignedEvent] =
      for {
        createdEvent <- CreatedEvent.toJson(v.getCreatedEvent)
      } yield v
        .into[JsAssignedEvent]
        .withFieldConst(_.createdEvent, createdEvent)
        .transform

    override def fromJson(
        jsObj: JsAssignedEvent
    )(implicit traceContext: TraceContext): Future[AssignedEvent] =
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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
        traceContext: TraceContext
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

  object GetUpdateTreesResponseLegacy
      extends ProtocolConverter[
        LegacyDTOs.GetUpdateTreesResponse,
        JsGetUpdateTreesResponse,
      ] {
    def toJson(
        value: LegacyDTOs.GetUpdateTreesResponse
    )(implicit
        traceContext: TraceContext
    ): Future[JsGetUpdateTreesResponse] =
      ((value.update match {
        case LegacyDTOs.GetUpdateTreesResponse.Update.Empty =>
          illegalValue(LegacyDTOs.GetUpdateTreesResponse.Update.Empty.toString())
        case LegacyDTOs.GetUpdateTreesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdateTree.OffsetCheckpoint(value))
        case LegacyDTOs.GetUpdateTreesResponse.Update.TransactionTree(value) =>
          TransactionTreeLegacy.toJson(value).map(JsUpdateTree.TransactionTree.apply)
        case LegacyDTOs.GetUpdateTreesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdateTree.Reassignment.apply)
      }): Future[JsUpdateTree.Update]).map(update => JsGetUpdateTreesResponse(update))

    def fromJson(
        jsObj: JsGetUpdateTreesResponse
    )(implicit
        traceContext: TraceContext
    ): Future[LegacyDTOs.GetUpdateTreesResponse] =
      (jsObj.update match {
        case JsUpdateTree.OffsetCheckpoint(value) =>
          Future.successful(
            LegacyDTOs.GetUpdateTreesResponse.Update.OffsetCheckpoint(value)
          )
        case JsUpdateTree.Reassignment(value) =>
          Reassignment
            .fromJson(value)
            .map(LegacyDTOs.GetUpdateTreesResponse.Update.Reassignment.apply)
        case JsUpdateTree.TransactionTree(value) =>
          TransactionTreeLegacy
            .fromJson(value)
            .map(LegacyDTOs.GetUpdateTreesResponse.Update.TransactionTree.apply)
      }).map((u: LegacyDTOs.GetUpdateTreesResponse.Update) => LegacyDTOs.GetUpdateTreesResponse(u))
  }

  object GetTransactionTreeResponseLegacy
      extends ProtocolConverter[
        LegacyDTOs.GetTransactionTreeResponse,
        JsGetTransactionTreeResponse,
      ] {
    def toJson(
        obj: LegacyDTOs.GetTransactionTreeResponse
    )(implicit
        traceContext: TraceContext
    ): Future[JsGetTransactionTreeResponse] =
      TransactionTreeLegacy
        .toJson(obj.transaction.getOrElse(invalidArgument("empty", "non-empty transaction")))
        .map(JsGetTransactionTreeResponse.apply)

    def fromJson(treeResponse: JsGetTransactionTreeResponse)(implicit
        traceContext: TraceContext
    ): Future[LegacyDTOs.GetTransactionTreeResponse] =
      TransactionTreeLegacy
        .fromJson(treeResponse.transaction)
        .map(tree => LegacyDTOs.GetTransactionTreeResponse(Some(tree)))

  }

  object GetTransactionResponseLegacy
      extends ProtocolConverter[
        LegacyDTOs.GetTransactionResponse,
        JsGetTransactionResponse,
      ] {
    def toJson(obj: LegacyDTOs.GetTransactionResponse)(implicit
        traceContext: TraceContext
    ): Future[JsGetTransactionResponse] =
      Transaction
        .toJson(obj.transaction.getOrElse(invalidArgument("empty", "non-empty transaction")))
        .map(JsGetTransactionResponse.apply)

    def fromJson(obj: JsGetTransactionResponse)(implicit
        traceContext: TraceContext
    ): Future[LegacyDTOs.GetTransactionResponse] =
      Transaction
        .fromJson(obj.transaction)
        .map(tr => LegacyDTOs.GetTransactionResponse(Some(tr)))
  }

  object PrepareSubmissionRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.PrepareSubmissionRequest,
        JsPrepareSubmissionRequest,
      ] {
    def fromJson(obj: JsPrepareSubmissionRequest)(implicit
        traceContext: TraceContext
    ): Future[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest] = for {
      commandsWithTranscodingPackageIds <- transcodePackageIdResolver
        .resolveDecodingPackageIdsForJsonCommands(
          jsCommands = obj.commands,
          actAs = obj.actAs,
          packageIdSelectionPreference = obj.packageIdSelectionPreference,
          synchronizerIdO = Option(obj.synchronizerId).filter(_.nonEmpty),
        )
      transcodedCommands <- SeqCommands.fromJson(commandsWithTranscodingPackageIds)
      prefetchContractKeys <- obj.prefetchContractKeys.map(PrefetchContractKey.fromJson).sequence
    } yield obj
      .into[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest]
      .withFieldConst(_.commands, transcodedCommands.map(lapi.commands.Command(_)))
      .withFieldConst(_.prefetchContractKeys, prefetchContractKeys)
      .transform

    override def toJson(
        lapiObj: PrepareSubmissionRequest
    )(implicit
        traceContext: TraceContext
    ): Future[JsPrepareSubmissionRequest] = for {
      commandsWithTranscodingPackageIds <- transcodePackageIdResolver
        .resolveDecodingPackageIdsForGrpcCommands(
          grpcCommands = lapiObj.commands.map(_.command),
          actAs = lapiObj.actAs,
          packageIdSelectionPreference = lapiObj.packageIdSelectionPreference,
          synchronizerIdO = Option(lapiObj.synchronizerId).filter(_.nonEmpty),
        )
      transcodedCommands <- SeqCommands.toJson(commandsWithTranscodingPackageIds)
      prefetchContractKeys <- lapiObj.prefetchContractKeys.map(PrefetchContractKey.toJson).sequence
    } yield lapiObj
      .into[JsPrepareSubmissionRequest]
      .withFieldConst(_.commands, transcodedCommands)
      .withFieldConst(_.prefetchContractKeys, prefetchContractKeys)
      .transform
  }

  object PrepareSubmissionResponse
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.PrepareSubmissionResponse,
        JsPrepareSubmissionResponse,
      ] {
    def toJson(
        obj: lapi.interactive.interactive_submission_service.PrepareSubmissionResponse
    )(implicit traceContext: TraceContext): Future[JsPrepareSubmissionResponse] =
      Future.successful(
        obj
          .into[JsPrepareSubmissionResponse]
          .withFieldConst(_.preparedTransaction, obj.preparedTransaction.map(_.toByteString))
          .transform
      )

    override def fromJson(
        jsObj: JsPrepareSubmissionResponse
    )(implicit traceContext: TraceContext): Future[PrepareSubmissionResponse] =
      Future.successful(
        jsObj
          .into[PrepareSubmissionResponse]
          .withFieldConst(
            _.preparedTransaction,
            jsObj.preparedTransaction.map(_.toByteArray).map(PreparedTransaction.parseFrom),
          )
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
        traceContext: TraceContext
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

    override def toJson(
        lapi: ExecuteSubmissionRequest
    )(implicit traceContext: TraceContext): Future[JsExecuteSubmissionRequest] = Future.successful(
      lapi
        .into[JsExecuteSubmissionRequest]
        .withFieldConst(_.preparedTransaction, lapi.preparedTransaction.map(_.toByteString))
        .transform
    )
  }

  object ExecuteSubmissionAndWaitRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitRequest,
        JsExecuteSubmissionAndWaitRequest,
      ] {
    def fromJson(
        obj: JsExecuteSubmissionAndWaitRequest
    )(implicit
        traceContext: TraceContext
    ): Future[lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitRequest] =
      ExecuteSubmissionRequest
        .fromJson(obj.transformInto[JsExecuteSubmissionRequest])
        .map(
          _.transformInto[
            lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitRequest
          ]
        )

    override def toJson(
        protoRequest: lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitRequest
    )(implicit traceContext: TraceContext): Future[JsExecuteSubmissionAndWaitRequest] =
      for {
        json <- ExecuteSubmissionRequest.toJson(
          protoRequest.transformInto[ExecuteSubmissionRequest]
        )
      } yield json.transformInto[JsExecuteSubmissionAndWaitRequest]
  }

  object ExecuteSubmissionAndWaitForTransactionRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionRequest,
        JsExecuteSubmissionAndWaitForTransactionRequest,
      ] {
    def fromJson(
        obj: JsExecuteSubmissionAndWaitForTransactionRequest
    )(implicit traceContext: TraceContext): Future[
      lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionRequest
    ] =
      ExecuteSubmissionRequest
        .fromJson(obj.transformInto[JsExecuteSubmissionRequest])
        .map(
          _.into[
            lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionRequest
          ]
            .withFieldConst(_.transactionFormat, obj.transactionFormat)
            .transform
        )

    override def toJson(
        protoRequest: lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionRequest
    )(implicit
        traceContext: TraceContext
    ): Future[JsExecuteSubmissionAndWaitForTransactionRequest] =
      for {
        json <- ExecuteSubmissionRequest.toJson(
          protoRequest.transformInto[ExecuteSubmissionRequest]
        )
      } yield json
        .into[JsExecuteSubmissionAndWaitForTransactionRequest]
        .withFieldConst(_.transactionFormat, protoRequest.transactionFormat)
        .transform
  }

  object ExecuteSubmissionAndWaitForTransactionResponse
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionResponse,
        JsExecuteSubmissionAndWaitForTransactionResponse,
      ] {

    def toJson(
        response: lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionResponse
    )(implicit
        traceContext: TraceContext
    ): Future[JsExecuteSubmissionAndWaitForTransactionResponse] =
      for {
        transaction <- Transaction
          .toJson(response.getTransaction)
      } yield JsExecuteSubmissionAndWaitForTransactionResponse(transaction)

    def fromJson(
        jsResponse: JsExecuteSubmissionAndWaitForTransactionResponse
    )(implicit traceContext: TraceContext): Future[
      lapi.interactive.interactive_submission_service.ExecuteSubmissionAndWaitForTransactionResponse
    ] =
      for {
        transaction <- Transaction
          .fromJson(jsResponse.transaction)
      } yield lapi.interactive.interactive_submission_service
        .ExecuteSubmissionAndWaitForTransactionResponse(Some(transaction))
  }

  object AllocatePartyRequest
      extends ProtocolConverter[
        lapi.admin.party_management_service.AllocatePartyRequest,
        js.AllocatePartyRequest,
      ] {
    def fromJson(
        obj: js.AllocatePartyRequest
    )(implicit
        traceContext: TraceContext
    ): Future[lapi.admin.party_management_service.AllocatePartyRequest] =
      Future.successful(
        obj.into[lapi.admin.party_management_service.AllocatePartyRequest].transform
      )

    def toJson(
        obj: lapi.admin.party_management_service.AllocatePartyRequest
    )(implicit traceContext: TraceContext): Future[js.AllocatePartyRequest] = Future.successful(
      obj.into[js.AllocatePartyRequest].transform
    )
  }

  object PrefetchContractKey
      extends ProtocolConverter[
        lapi.commands.PrefetchContractKey,
        js.PrefetchContractKey,
      ] {
    def fromJson(obj: js.PrefetchContractKey)(implicit
        traceContext: TraceContext
    ): Future[lapi.commands.PrefetchContractKey] =
      for {
        contractKey <- obj.templateId
          .traverse(template => schemaProcessors.keyArgFromJsonToProto(template, obj.contractKey))
      } yield obj
        .into[lapi.commands.PrefetchContractKey]
        .withFieldConst(_.contractKey, contractKey)
        .transform

    def toJson(
        obj: lapi.commands.PrefetchContractKey
    )(implicit traceContext: TraceContext): Future[js.PrefetchContractKey] = for {
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
