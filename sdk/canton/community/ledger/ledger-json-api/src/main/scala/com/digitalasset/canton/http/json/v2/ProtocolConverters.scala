// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2 as lapi
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsContractEntry
import com.digitalasset.canton.http.json.v2.JsPrepareSubmissionRequest
import com.digitalasset.canton.http.json.v2.JsReassignmentEvent.JsReassignmentEvent
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsEvent,
  JsInterfaceView,
  JsStatus,
  JsTopologyEvent,
  JsTopologyTransaction,
  JsTransaction,
  JsTransactionTree,
  JsTreeEvent,
}
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.daml.lf.data.Ref
import com.google.rpc.status.Status
import ujson.StringRenderer
import ujson.circe.CirceJson

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait ProtocolConverter[LAPI, JS] {

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

class ProtocolConverters(schemaProcessors: SchemaProcessors)(implicit
    val executionContext: ExecutionContext
) {

  implicit def fromCirce(js: io.circe.Json): ujson.Value =
    ujson.read(CirceJson.transform(js, StringRenderer()).toString)

  implicit def toCirce(js: ujson.Value): io.circe.Json = CirceJson(js)

  def convertCommands(commands: Seq[JsCommand.Command])(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Future[Seq[lapi.commands.Command.Command]] = Future.sequence(commands.map {
    case JsCommand.CreateCommand(template_id, create_arguments) =>
      for {
        protoCreateArgsRecord <-
          schemaProcessors
            .contractArgFromJsonToProto(
              template = IdentifierConverter.fromJson(template_id),
              jsonArgsValue = create_arguments,
            )

      } yield lapi.commands.Command.Command.Create(
        lapi.commands.CreateCommand(
          templateId = Some(IdentifierConverter.fromJson(template_id)),
          createArguments = Some(protoCreateArgsRecord.getRecord),
        )
      )
    case JsCommand.ExerciseCommand(template_id, contract_id, choice, choice_argument) =>
      val lfChoiceName = Ref.ChoiceName.assertFromString(choice)
      for {
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            template = IdentifierConverter.fromJson(template_id),
            choiceName = lfChoiceName,
            jsonArgsValue = choice_argument,
          )
      } yield lapi.commands.Command.Command.Exercise(
        lapi.commands.ExerciseCommand(
          templateId = Some(IdentifierConverter.fromJson(template_id)),
          contractId = contract_id,
          choiceArgument = Some(choiceArgs),
          choice = choice,
        )
      )

    case cmd: JsCommand.ExerciseByKeyCommand =>
      for {
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            template = IdentifierConverter.fromJson(cmd.templateId),
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            jsonArgsValue = cmd.choiceArgument,
          )
        contractKey <-
          schemaProcessors.contractArgFromJsonToProto(
            template = IdentifierConverter.fromJson(cmd.templateId),
            jsonArgsValue = cmd.contractKey,
          )
      } yield lapi.commands.Command.Command.ExerciseByKey(
        lapi.commands.ExerciseByKeyCommand(
          templateId = Some(IdentifierConverter.fromJson(cmd.templateId)),
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
              template = IdentifierConverter.fromJson(cmd.templateId),
              jsonArgsValue = cmd.createArguments,
            )
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            template = IdentifierConverter.fromJson(cmd.templateId),
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            jsonArgsValue = cmd.choiceArgument,
          )
      } yield lapi.commands.Command.Command.CreateAndExercise(
        lapi.commands.CreateAndExerciseCommand(
          templateId = Some(IdentifierConverter.fromJson(cmd.templateId)),
          createArguments = Some(createArgs.getRecord),
          choice = cmd.choice,
          choiceArgument = Some(choiceArgs),
        )
      )
  })

  object Commands extends ProtocolConverter[lapi.commands.Commands, JsCommands] {

    def fromJson(jsCommands: JsCommands)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.commands.Commands] = {
      import jsCommands.*

      val convertedCommands = convertCommands(jsCommands.commands)
      convertedCommands
        .map(cc =>
          lapi.commands.Commands(
            workflowId = workflowId.getOrElse(""),
            userId = userId.getOrElse(""),
            commandId = commandId,
            commands = cc.map(lapi.commands.Command(_)),
            deduplicationPeriod = deduplicationPeriod.getOrElse(
              com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.Empty
            ),
            minLedgerTimeAbs = minLedgerTimeAbs,
            minLedgerTimeRel = minLedgerTimeRel,
            actAs = actAs,
            readAs = readAs,
            submissionId = submissionId.getOrElse(""),
            disclosedContracts = disclosedContracts,
            synchronizerId = synchronizerId.getOrElse(""),
            packageIdSelectionPreference = packageIdSelectionPreference,
            prefetchContractKeys = Nil,
          )
        )
    }

    def toJson(
        lapiCommands: lapi.commands.Commands
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsCommands] = {
      val jsCommands: Seq[Future[JsCommand.Command]] = lapiCommands.commands
        .map(_.command)
        .map {
          case lapi.commands.Command.Command.Empty =>
            illegalValue(lapi.commands.Command.Command.Empty.toString())
          case lapi.commands.Command.Command.Create(createCommand) =>
            for {
              contractArgs <- schemaProcessors.contractArgFromProtoToJson(
                createCommand.getTemplateId,
                createCommand.getCreateArguments,
              )
            } yield JsCommand.CreateCommand(
              IdentifierConverter.toJson(createCommand.getTemplateId),
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
              IdentifierConverter.toJson(exerciseCommand.getTemplateId),
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
              templateId = IdentifierConverter.toJson(cmd.getTemplateId),
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
              templateId = IdentifierConverter.toJson(cmd.getTemplateId),
              contractKey = contractKey,
              choice = cmd.choice,
              choiceArgument = choiceArgs,
            )
        }
      Future
        .sequence(jsCommands)
        .map(cmds =>
          JsCommands(
            commands = cmds,
            workflowId = Some(lapiCommands.workflowId),
            userId = Some(lapiCommands.userId),
            commandId = lapiCommands.commandId,
            deduplicationPeriod = Some(lapiCommands.deduplicationPeriod),
            disclosedContracts = lapiCommands.disclosedContracts,
            actAs = lapiCommands.actAs,
            readAs = lapiCommands.readAs,
            submissionId = Some(lapiCommands.submissionId),
            synchronizerId = Some(lapiCommands.synchronizerId),
            minLedgerTimeAbs = lapiCommands.minLedgerTimeAbs,
            minLedgerTimeRel = lapiCommands.minLedgerTimeRel,
            packageIdSelectionPreference = lapiCommands.packageIdSelectionPreference,
          )
        )
    }
  }

  object InterfaceView extends ProtocolConverter[lapi.event.InterfaceView, JsInterfaceView] {

    def fromJson(
        iview: JsInterfaceView
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.event.InterfaceView] = for {
      record <- iview.viewValue
        .map { v =>
          schemaProcessors
            .contractArgFromJsonToProto(
              IdentifierConverter.fromJson(iview.interfaceId),
              v,
            )
            .map(_.getRecord)
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))
    } yield lapi.event.InterfaceView(
      interfaceId = Some(IdentifierConverter.fromJson(iview.interfaceId)),
      viewStatus = Some(JsStatusConverter.fromJson(iview.viewStatus)),
      viewValue = record,
    )

    def toJson(
        obj: lapi.event.InterfaceView
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsInterfaceView] =
      for {
        record <- schemaProcessors.contractArgFromProtoToJson(
          obj.getInterfaceId,
          obj.getViewValue,
        )
      } yield JsInterfaceView(
        interfaceId = IdentifierConverter.toJson(obj.getInterfaceId),
        viewStatus = JsStatusConverter.toJson(obj.getViewStatus),
        viewValue = obj.viewValue.map(_ => record),
      )
  }

  object Event extends ProtocolConverter[lapi.event.Event.Event, JsEvent.Event] {
    def toJson(event: lapi.event.Event.Event)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.event.Event.Event] = event match {
      case createdEvent: JsEvent.CreatedEvent =>
        CreatedEvent.fromJson(createdEvent).map(lapi.event.Event.Event.Created(_))
      case archivedEvent: JsEvent.ArchivedEvent =>
        Future.successful(lapi.event.Event.Event.Archived(ArchivedEvent.fromJson(archivedEvent)))
      case exercisedEvent: JsEvent.ExercisedEvent =>
        ExercisedEvent.fromJson(exercisedEvent).map(lapi.event.Event.Event.Exercised(_))
    }
  }

  object TopologyEvent
      extends ProtocolConverter[
        lapi.topology_transaction.TopologyEvent,
        JsTopologyEvent.Event,
      ] {
    def toJson(
        event: lapi.topology_transaction.TopologyEvent.Event
    ): Future[JsTopologyEvent.Event] =
      event match {
        case lapi.topology_transaction.TopologyEvent.Event.Empty =>
          illegalValue(lapi.topology_transaction.TopologyEvent.Event.Empty.toString())
        case lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationChanged(value) =>
          Future(ParticipantAuthorizationChanged.toJson(value))
        case lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationRevoked(value) =>
          Future(ParticipantAuthorizationRevoked.toJson(value))
        case lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationAdded(value) =>
          Future(ParticipantAuthorizationAdded.toJson(value))
      }

    def fromJson(
        event: JsTopologyEvent.Event
    ): Future[lapi.topology_transaction.TopologyEvent.Event] = event match {
      case added: JsTopologyEvent.ParticipantAuthorizationAdded =>
        Future(
          lapi.topology_transaction.TopologyEvent.Event
            .ParticipantAuthorizationAdded(ParticipantAuthorizationAdded.fromJson(added))
        )
      case changed: JsTopologyEvent.ParticipantAuthorizationChanged =>
        Future(
          lapi.topology_transaction.TopologyEvent.Event
            .ParticipantAuthorizationChanged(ParticipantAuthorizationChanged.fromJson(changed))
        )
      case revoked: JsTopologyEvent.ParticipantAuthorizationRevoked =>
        Future(
          lapi.topology_transaction.TopologyEvent.Event
            .ParticipantAuthorizationRevoked(ParticipantAuthorizationRevoked.fromJson(revoked))
        )
    }
  }

  object Transaction extends ProtocolConverter[lapi.transaction.Transaction, JsTransaction] {

    def toJson(v: lapi.transaction.Transaction)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsTransaction] =
      Future
        .sequence(v.events.map(e => Event.toJson(e.event)))
        .map(ev =>
          JsTransaction(
            updateId = v.updateId,
            commandId = v.commandId,
            workflowId = v.workflowId,
            effectiveAt = v.getEffectiveAt,
            events = ev,
            offset = v.offset,
            synchronizerId = v.synchronizerId,
            traceContext = v.traceContext,
            recordTime = v.getRecordTime,
          )
        )

    def fromJson(v: JsTransaction)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.transaction.Transaction] = Future
      .sequence(v.events.map(e => Event.fromJson(e)))
      .map { ev =>
        lapi.transaction.Transaction(
          updateId = v.updateId,
          commandId = v.commandId,
          workflowId = v.workflowId,
          effectiveAt = Some(v.effectiveAt),
          events = ev.map(lapi.event.Event(_)),
          offset = v.offset,
          synchronizerId = v.synchronizerId,
          traceContext = v.traceContext,
          recordTime = Some(v.recordTime),
        )
      }
  }

  object TopologyTransaction
      extends ProtocolConverter[
        lapi.topology_transaction.TopologyTransaction,
        JsTopologyTransaction,
      ] {

    def toJson(v: lapi.topology_transaction.TopologyTransaction): Future[JsTopologyTransaction] =
      Future
        .sequence(v.events.map(e => TopologyEvent.toJson(e.event)))
        .map(ev =>
          JsTopologyTransaction(
            updateId = v.updateId,
            events = ev,
            offset = v.offset,
            synchronizerId = v.synchronizerId,
            traceContext = v.traceContext,
            recordTime = v.getRecordTime,
          )
        )

    def fromJson(v: JsTopologyTransaction): Future[lapi.topology_transaction.TopologyTransaction] =
      Future
        .sequence(v.events.map(e => TopologyEvent.fromJson(e)))
        .map { ev =>
          lapi.topology_transaction.TopologyTransaction(
            updateId = v.updateId,
            events = ev.map(lapi.topology_transaction.TopologyEvent(_)),
            offset = v.offset,
            synchronizerId = v.synchronizerId,
            traceContext = v.traceContext,
            recordTime = Some(v.recordTime),
          )
        }
  }

  object TransactionTree
      extends ProtocolConverter[lapi.transaction.TransactionTree, JsTransactionTree] {
    def toJson(
        lapiTransactionTree: lapi.transaction.TransactionTree
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsTransactionTree] = {
      val jsEventsById = lapiTransactionTree.eventsById.view
        .mapValues(_.kind)
        .mapValues[Future[JsTreeEvent.TreeEvent]] {
          case lapi.transaction.TreeEvent.Kind.Empty =>
            illegalValue(lapi.transaction.TreeEvent.Kind.Empty.toString())
          case lapi.transaction.TreeEvent.Kind.Created(created) =>
            CreatedEvent.toJson(created).map(JsTreeEvent.CreatedTreeEvent(_))
          case lapi.transaction.TreeEvent.Kind.Exercised(exercised) =>
            ExercisedEvent.toJson(exercised).map(JsTreeEvent.ExercisedTreeEvent(_))
        }
      Future
        .traverse(jsEventsById.toSeq) { case (key, fv) =>
          fv.map(key -> _)
        }
        .map(_.toMap)
        .map(jsEvents =>
          JsTransactionTree(
            updateId = lapiTransactionTree.updateId,
            commandId = lapiTransactionTree.commandId,
            workflowId = lapiTransactionTree.workflowId,
            effectiveAt = lapiTransactionTree.effectiveAt,
            offset = lapiTransactionTree.offset,
            eventsById = jsEvents,
            synchronizerId = lapiTransactionTree.synchronizerId,
            traceContext = lapiTransactionTree.traceContext,
            recordTime = lapiTransactionTree.getRecordTime,
          )
        )
    }

    def fromJson(
        jsTransactionTree: JsTransactionTree
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.transaction.TransactionTree] = {
      val lapiEventsById = jsTransactionTree.eventsById.view
        .mapValues {
          case JsTreeEvent.CreatedTreeEvent(created) =>
            CreatedEvent
              .fromJson(created)
              .map(ev => lapi.transaction.TreeEvent(lapi.transaction.TreeEvent.Kind.Created(ev)))

          case JsTreeEvent.ExercisedTreeEvent(exercised) =>
            ExercisedEvent
              .fromJson(exercised)
              .map(ev => lapi.transaction.TreeEvent(lapi.transaction.TreeEvent.Kind.Exercised(ev)))
        }
      Future
        .traverse(lapiEventsById.toSeq) { case (key, fv) =>
          fv.map(key -> _)
        }
        .map(events =>
          lapi.transaction.TransactionTree(
            eventsById = events.toMap,
            offset = jsTransactionTree.offset,
            updateId = jsTransactionTree.updateId,
            commandId = jsTransactionTree.commandId,
            workflowId = jsTransactionTree.workflowId,
            effectiveAt = jsTransactionTree.effectiveAt,
            synchronizerId = jsTransactionTree.synchronizerId,
            traceContext = jsTransactionTree.traceContext,
            recordTime = Some(jsTransactionTree.recordTime),
          )
        )
    }
  }

  object SubmitAndWaitTransactionTreeResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForTransactionTreeResponse,
        JsSubmitAndWaitForTransactionTreeResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitForTransactionTreeResponse
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsSubmitAndWaitForTransactionResponse] =
      Transaction
        .toJson(response.getTransaction)
        .map(tx =>
          JsSubmitAndWaitForTransactionResponse(
            transaction = tx
          )
        )

    def fromJson(
        jsResponse: JsSubmitAndWaitForTransactionResponse
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.command_service.SubmitAndWaitForTransactionResponse] = Transaction
      .fromJson(jsResponse.transaction)
      .map(tx =>
        lapi.command_service.SubmitAndWaitForTransactionResponse(
          transaction = Some(tx)
        )
      )
  }

  object SubmitAndWaitForTransactionRequest
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitRequest,
        JsSubmitAndWaitForTransactionRequest,
      ] {

    def toJson(
        request: lapi.command_service.SubmitAndWaitForTransactionRequest
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsSubmitAndWaitForTransactionRequest] =
      Commands
        .toJson(request.getCommands)
        .map(commands =>
          JsSubmitAndWaitForTransactionRequest(
            commands = commands,
            transactionFormat = request.getTransactionFormat,
          )
        )

    def fromJson(
        jsRequest: JsSubmitAndWaitForTransactionRequest
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.command_service.SubmitAndWaitForTransactionRequest] = Commands
      .fromJson(jsRequest.commands)
      .map(commands =>
        lapi.command_service.SubmitAndWaitForTransactionRequest(
          commands = Some(commands),
          transactionFormat = Some(jsRequest.transactionFormat),
        )
      )
  }

  object GetEventsByContractIdResponse
      extends ProtocolConverter[
        lapi.event_query_service.GetEventsByContractIdResponse,
        JsGetEventsByContractIdResponse,
      ] {
    def toJson(
        response: lapi.event_query_service.GetEventsByContractIdResponse
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
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

  object ArchivedEvent extends ProtocolConverter[lapi.event.ArchivedEvent, JsEvent.ArchivedEvent] {
    def toJson(e: lapi.event.ArchivedEvent): JsEvent.ArchivedEvent = JsEvent.ArchivedEvent(
      offset = e.offset,
      nodeId = e.nodeId,
      contractId = e.contractId,
      templateId = IdentifierConverter.toJson(e.getTemplateId),
      witnessParties = e.witnessParties,
      packageName = e.packageName,
      implementedInterfaces = e.implementedInterfaces.map(IdentifierConverter.toJson),
    )

    def fromJson(ev: JsEvent.ArchivedEvent): lapi.event.ArchivedEvent = lapi.event.ArchivedEvent(
      offset = ev.offset,
      nodeId = ev.nodeId,
      contractId = ev.contractId,
      templateId = Some(IdentifierConverter.fromJson(ev.templateId)),
      witnessParties = ev.witnessParties,
      packageName = ev.packageName,
      implementedInterfaces = ev.implementedInterfaces.map(IdentifierConverter.fromJson),
    )
  }

  object CreatedEvent extends ProtocolConverter[lapi.event.CreatedEvent, JsEvent.CreatedEvent] {
    def toJson(created: lapi.event.CreatedEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsEvent.CreatedEvent] =
      for {
        contractKey <- created.contractKey
          .map(ck =>
            schemaProcessors
              .keyArgFromProtoToJson(
                created.getTemplateId,
                ck,
              )
              .map(Some(_))
          )
          .getOrElse(Future(None))
        createdArgs <- created.createArguments
          .map(ca =>
            schemaProcessors
              .contractArgFromProtoToJson(
                created.getTemplateId,
                ca,
              )
              .map(Some(_))
          )
          .getOrElse(Future(None))
        interfaceViews <- Future.sequence(created.interfaceViews.map(InterfaceView.toJson))
      } yield JsEvent.CreatedEvent(
        offset = created.offset,
        nodeId = created.nodeId,
        contractId = created.contractId,
        templateId = IdentifierConverter.toJson(created.getTemplateId),
        contractKey = contractKey.map(toCirce),
        createArgument = createdArgs.map(toCirce),
        createdEventBlob = created.createdEventBlob,
        interfaceViews = interfaceViews,
        witnessParties = created.witnessParties,
        signatories = created.signatories,
        observers = created.observers,
        createdAt = created.getCreatedAt,
        packageName = created.packageName,
      )

    def fromJson(createdEvent: JsEvent.CreatedEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.event.CreatedEvent] = {
      val templateId = IdentifierConverter.fromJson(createdEvent.templateId)
      for {
        contractKey <- createdEvent.contractKey
          .map(key =>
            schemaProcessors
              .keyArgFromJsonToProto(templateId, key)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        createArgs <- createdEvent.createArgument
          .map(args =>
            schemaProcessors
              .contractArgFromJsonToProto(templateId, args)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        interfaceViews <- Future.sequence(createdEvent.interfaceViews.map(InterfaceView.fromJson))
      } yield lapi.event.CreatedEvent(
        offset = createdEvent.offset,
        nodeId = createdEvent.nodeId,
        contractId = createdEvent.contractId,
        templateId = Some(templateId),
        contractKey = contractKey,
        createArguments = createArgs.map(_.getRecord),
        createdEventBlob = createdEvent.createdEventBlob,
        interfaceViews = interfaceViews,
        witnessParties = createdEvent.witnessParties,
        signatories = createdEvent.signatories,
        observers = createdEvent.observers,
        createdAt = Some(createdEvent.createdAt),
        packageName = createdEvent.packageName,
      )
    }

  }

  object ExercisedEvent
      extends ProtocolConverter[lapi.event.ExercisedEvent, JsEvent.ExercisedEvent] {
    def toJson(exercised: lapi.event.ExercisedEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
      } yield JsEvent.ExercisedEvent(
        offset = exercised.offset,
        nodeId = exercised.nodeId,
        contractId = exercised.contractId,
        templateId = exercised.getTemplateId,
        interfaceId = exercised.interfaceId,
        choice = exercised.choice,
        choiceArgument = choiceArgs,
        actingParties = exercised.actingParties,
        consuming = exercised.consuming,
        witnessParties = exercised.witnessParties,
        lastDescendantNodeId = exercised.lastDescendantNodeId,
        exerciseResult = exerciseResult,
        packageName = exercised.packageName,
        implementedInterfaces = exercised.implementedInterfaces,
      )

    def fromJson(exericisedEvent: JsEvent.ExercisedEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.event.ExercisedEvent] =
      for {
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            exericisedEvent.interfaceId.getOrElse(exericisedEvent.templateId),
            Ref.ChoiceName.assertFromString(exericisedEvent.choice),
            exericisedEvent.choiceArgument,
          )
        choiceResult <-
          schemaProcessors
            .exerciseResultFromJsonToProto(
              exericisedEvent.interfaceId.getOrElse(exericisedEvent.templateId),
              Ref.ChoiceName.assertFromString(exericisedEvent.choice),
              exericisedEvent.exerciseResult,
            )
      } yield lapi.event.ExercisedEvent(
        offset = exericisedEvent.offset,
        nodeId = exericisedEvent.nodeId,
        contractId = exericisedEvent.contractId,
        templateId = Some(exericisedEvent.templateId),
        interfaceId = exericisedEvent.interfaceId,
        choice = exericisedEvent.choice,
        choiceArgument = Some(choiceArgs),
        actingParties = exericisedEvent.actingParties,
        consuming = exericisedEvent.consuming,
        witnessParties = exericisedEvent.witnessParties,
        lastDescendantNodeId = exericisedEvent.lastDescendantNodeId,
        exerciseResult = choiceResult,
        packageName = exericisedEvent.packageName,
        implementedInterfaces = exericisedEvent.implementedInterfaces,
      )

  }

  object AssignedEvent
      extends ProtocolConverter[
        lapi.reassignment.AssignedEvent,
        JsAssignedEvent,
      ] {

    def toJson(v: lapi.reassignment.AssignedEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsAssignedEvent] =
      CreatedEvent
        .toJson(v.getCreatedEvent)
        .map(ev =>
          JsAssignedEvent(
            source = v.source,
            target = v.target,
            unassignId = v.unassignId,
            submitter = v.submitter,
            reassignmentCounter = v.reassignmentCounter,
            createdEvent = ev,
          )
        )
  }

  object ParticipantAuthorizationChanged
      extends ProtocolConverter[
        lapi.topology_transaction.ParticipantAuthorizationChanged,
        JsTopologyEvent.ParticipantAuthorizationChanged,
      ] {
    def toJson(
        e: lapi.topology_transaction.ParticipantAuthorizationChanged
    ): JsTopologyEvent.ParticipantAuthorizationChanged =
      JsTopologyEvent.ParticipantAuthorizationChanged(
        partyId = e.partyId,
        participantId = e.participantId,
        participantPermission = e.participantPermission.value,
      )

    def fromJson(
        ev: JsTopologyEvent.ParticipantAuthorizationChanged
    ): lapi.topology_transaction.ParticipantAuthorizationChanged =
      lapi.topology_transaction.ParticipantAuthorizationChanged(
        partyId = ev.partyId,
        participantId = ev.participantId,
        participantPermission =
          lapi.state_service.ParticipantPermission.fromValue(ev.participantPermission),
      )
  }

  object ParticipantAuthorizationAdded
      extends ProtocolConverter[
        lapi.topology_transaction.ParticipantAuthorizationAdded,
        JsTopologyEvent.ParticipantAuthorizationAdded,
      ] {
    def toJson(
        e: lapi.topology_transaction.ParticipantAuthorizationAdded
    ): JsTopologyEvent.ParticipantAuthorizationAdded =
      JsTopologyEvent.ParticipantAuthorizationAdded(
        partyId = e.partyId,
        participantId = e.participantId,
        participantPermission = e.participantPermission.value,
      )

    def fromJson(
        ev: JsTopologyEvent.ParticipantAuthorizationAdded
    ): lapi.topology_transaction.ParticipantAuthorizationAdded =
      lapi.topology_transaction.ParticipantAuthorizationAdded(
        partyId = ev.partyId,
        participantId = ev.participantId,
        participantPermission =
          lapi.state_service.ParticipantPermission.fromValue(ev.participantPermission),
      )
  }

  object ParticipantAuthorizationRevoked
      extends ProtocolConverter[
        lapi.topology_transaction.ParticipantAuthorizationRevoked,
        JsTopologyEvent.ParticipantAuthorizationRevoked,
      ] {
    def toJson(
        e: lapi.topology_transaction.ParticipantAuthorizationRevoked
    ): JsTopologyEvent.ParticipantAuthorizationRevoked =
      JsTopologyEvent.ParticipantAuthorizationRevoked(
        partyId = e.partyId,
        participantId = e.participantId,
      )

    def fromJson(
        ev: JsTopologyEvent.ParticipantAuthorizationRevoked
    ): lapi.topology_transaction.ParticipantAuthorizationRevoked =
      lapi.topology_transaction.ParticipantAuthorizationRevoked(
        partyId = ev.partyId,
        participantId = ev.participantId,
      )
  }

  object ContractEntry
      extends ProtocolConverter[
        lapi.state_service.GetActiveContractsResponse.ContractEntry,
        JsContractEntry,
      ] {
    def toJson(
        v: lapi.state_service.GetActiveContractsResponse.ContractEntry
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsContractEntry] =
      v match {
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty =>
          Future(JsContractEntry.JsEmpty)
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsContractEntry.JsActiveContract(
                createdEvent = ce,
                synchronizerId = value.synchronizerId,
                reassignmentCounter = value.reassignmentCounter,
              )
            )
        case lapi.state_service.GetActiveContractsResponse.ContractEntry
              .IncompleteUnassigned(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsContractEntry.JsIncompleteUnassigned(
                createdEvent = ce,
                unassignedEvent = value.getUnassignedEvent,
              )
            )
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.state_service.GetActiveContractsResponse.ContractEntry] = jsContractEntry match {
      case JsContractEntry.JsEmpty =>
        Future(lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty)
      case JsContractEntry.JsIncompleteAssigned(assigned_event) =>
        CreatedEvent
          .fromJson(assigned_event.createdEvent)
          .map(ce =>
            lapi.state_service.GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
              new lapi.state_service.IncompleteAssigned(
                Some(
                  lapi.reassignment.AssignedEvent(
                    source = assigned_event.source,
                    target = assigned_event.target,
                    unassignId = assigned_event.unassignId,
                    submitter = assigned_event.submitter,
                    reassignmentCounter = assigned_event.reassignmentCounter,
                    createdEvent = Some(ce),
                  )
                )
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsGetActiveContractsResponse] =
      ContractEntry
        .toJson(v.contractEntry)
        .map(ce =>
          JsGetActiveContractsResponse(
            workflowId = v.workflowId,
            contractEntry = ce,
          )
        )

    def fromJson(
        v: JsGetActiveContractsResponse
    )(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsReassignmentEvent] =
      v match {
        case lapi.reassignment.ReassignmentEvent.Event.Empty =>
          illegalValue(lapi.reassignment.ReassignmentEvent.Event.Empty.toString())
        case lapi.reassignment.ReassignmentEvent.Event.Unassigned(value) =>
          Future(JsReassignmentEvent.JsUnassignedEvent(value))
        case lapi.reassignment.ReassignmentEvent.Event.Assigned(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsReassignmentEvent.JsAssignmentEvent(
                source = value.source,
                target = value.target,
                unassignId = value.unassignId,
                submitter = value.submitter,
                reassignmentCounter = value.reassignmentCounter,
                createdEvent = ce,
              )
            )
      }

    def fromJson(jsObj: JsReassignmentEvent)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.reassignment.ReassignmentEvent.Event] =
      jsObj match {
        case event: JsReassignmentEvent.JsAssignmentEvent =>
          CreatedEvent
            .fromJson(event.createdEvent)
            .map(ce =>
              lapi.reassignment.ReassignmentEvent.Event.Assigned(
                value = lapi.reassignment.AssignedEvent(
                  source = event.source,
                  target = event.target,
                  unassignId = event.unassignId,
                  submitter = event.submitter,
                  reassignmentCounter = event.reassignmentCounter,
                  createdEvent = Some(ce),
                )
              )
            )

        case JsReassignmentEvent.JsUnassignedEvent(value) =>
          Future.successful(lapi.reassignment.ReassignmentEvent.Event.Unassigned(value))
      }

  }
  object Reassignment extends ProtocolConverter[lapi.reassignment.Reassignment, JsReassignment] {
    def toJson(v: lapi.reassignment.Reassignment)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsReassignment] = v.events
      .traverse(e => ReassignmentEvent.toJson(e.event))
      .map(e =>
        JsReassignment(
          updateId = v.updateId,
          commandId = v.commandId,
          workflowId = v.workflowId,
          offset = v.offset,
          events = e,
          traceContext = v.traceContext,
          recordTime = v.getRecordTime,
        )
      )

    def fromJson(value: JsReassignment)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.reassignment.Reassignment] =
      value.events
        .traverse(e => ReassignmentEvent.fromJson(e))
        .map(re =>
          lapi.reassignment.Reassignment(
            updateId = value.updateId,
            commandId = value.commandId,
            workflowId = value.workflowId,
            offset = value.offset,
            events = re.map(lapi.reassignment.ReassignmentEvent(_)),
            traceContext = value.traceContext,
            recordTime = Some(value.recordTime),
          )
        )
  }

  object GetUpdatesResponse
      extends ProtocolConverter[lapi.update_service.GetUpdatesResponse, JsGetUpdatesResponse] {
    def toJson(obj: lapi.update_service.GetUpdatesResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
          TopologyTransaction.toJson(value).map(JsUpdate.TopologyTransaction.apply)
      }): Future[JsUpdate.Update]).map(update => JsGetUpdatesResponse(update))

    def fromJson(obj: JsGetUpdatesResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
          TopologyTransaction
            .fromJson(value)
            .map(lapi.update_service.GetUpdatesResponse.Update.TopologyTransaction.apply)
      }).map(lapi.update_service.GetUpdatesResponse(_))
  }

  object GetUpdateResponse
      extends ProtocolConverter[lapi.update_service.GetUpdateResponse, JsGetUpdatesResponse] {
    def toJson(obj: lapi.update_service.GetUpdateResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsGetUpdateResponse] =
      ((obj.update match {
        case lapi.update_service.GetUpdateResponse.Update.Empty =>
          illegalValue(lapi.update_service.GetUpdateResponse.Update.Empty.toString())
        case lapi.update_service.GetUpdateResponse.Update.Transaction(value) =>
          Transaction.toJson(value).map(JsUpdate.Transaction.apply)
        case lapi.update_service.GetUpdateResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdate.Reassignment.apply)
        case lapi.update_service.GetUpdateResponse.Update.TopologyTransaction(value) =>
          TopologyTransaction.toJson(value).map(JsUpdate.TopologyTransaction.apply)
      }): Future[JsUpdate.Update]).map(update => JsGetUpdateResponse(update))

    def fromJson(obj: JsGetUpdateResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
          TopologyTransaction
            .fromJson(value)
            .map(lapi.update_service.GetUpdateResponse.Update.TopologyTransaction.apply)
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
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsGetTransactionTreeResponse] =
      TransactionTree.toJson(obj.getTransaction).map(JsGetTransactionTreeResponse.apply)

    def fromJson(treeResponse: JsGetTransactionTreeResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
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
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[JsGetTransactionResponse] =
      Transaction.toJson(obj.getTransaction).map(JsGetTransactionResponse.apply)

    def fromJson(obj: JsGetTransactionResponse)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.update_service.GetTransactionResponse] =
      Transaction
        .fromJson(obj.transaction)
        .map(tr => lapi.update_service.GetTransactionResponse(Some(tr)))
  }

  object PrepareSubmissionRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.PrepareSubmissionRequest,
        JsPrepareSubmissionRequest,
      ] {
    def fromJson(obj: JsPrepareSubmissionRequest)(implicit
        contextualizedErrorLogger: ContextualizedErrorLogger
    ): Future[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest] = for {
      commands <- convertCommands(obj.commands)
    } yield lapi.interactive.interactive_submission_service.PrepareSubmissionRequest(
      userId = obj.userId,
      commandId = obj.commandId,
      commands = commands.map(lapi.commands.Command(_)),
      minLedgerTime = obj.minLedgerTime,
      actAs = obj.actAs,
      readAs = obj.readAs,
      disclosedContracts = obj.disclosedContracts,
      synchronizerId = obj.synchronizerId,
      packageIdSelectionPreference = obj.packageIdSelectionPreference,
      verboseHashing = obj.verboseHashing,
      prefetchContractKeys = Nil,
    )
  }

  object PrepareSubmissionResponse
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.PrepareSubmissionResponse,
        JsPrepareSubmissionResponse,
      ] {
    def toJson(
        obj: lapi.interactive.interactive_submission_service.PrepareSubmissionResponse
    ): Future[JsPrepareSubmissionResponse] = Future.successful(
      JsPrepareSubmissionResponse(
        preparedTransaction = obj.preparedTransaction.map(_.toByteString),
        preparedTransactionHash = obj.preparedTransactionHash,
        hashingSchemeVersion = obj.hashingSchemeVersion,
        hashingDetails = obj.hashingDetails,
      )
    )
  }

  object ExecuteSubmissionRequest
      extends ProtocolConverter[
        lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest,
        JsExecuteSubmissionRequest,
      ] {
    def fromJson(
        obj: JsExecuteSubmissionRequest
    ): Future[lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest] =
      Future {
        val preparedTransaction = obj.preparedTransaction.map { proto =>
          ProtoConverter
            .protoParser(
              lapi.interactive.interactive_submission_service.PreparedTransaction.parseFrom
            )(proto)
            .getOrElse(jsFail("Cannot parse prepared_transaction"))
        }
        lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest(
          preparedTransaction = preparedTransaction,
          partySignatures = obj.partySignatures,
          deduplicationPeriod = obj.deduplicationPeriod,
          submissionId = obj.submissionId,
          userId = obj.userId,
          hashingSchemeVersion = obj.hashingSchemeVersion,
        )
      }
  }

}

object IdentifierConverter extends ProtocolConverter[lapi.value.Identifier, String] {
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

object JsStatusConverter extends ProtocolConverter[com.google.rpc.status.Status, JsStatus] {
  def toJson(lapi: Status): JsStatus = JsStatus(
    code = lapi.code,
    message = lapi.message,
    details = lapi.details,
  )

  def fromJson(status: JsStatus): Status = Status(
    code = status.code,
    message = status.message,
    details = status.details,
  )
}
