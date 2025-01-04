// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.ContextualizedErrorLogger
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
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.daml.lf.data.Ref
import com.google.rpc.status.Status
import ujson.StringRenderer
import ujson.circe.CirceJson

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait ProtocolConverter[LAPI, JS] {
  def jsFail(err: String): Nothing = throw new IllegalArgumentException(
    err
  ) // TODO (i19398) improve error handling
}

class ProtocolConverters(schemaProcessors: SchemaProcessors)(implicit
    val executionContext: ExecutionContext
) {

  implicit def fromCirce(js: io.circe.Json): ujson.Value =
    ujson.read(CirceJson.transform(js, StringRenderer()).toString)

  implicit def toCirce(js: ujson.Value): io.circe.Json = CirceJson(js)

  def convertCommands(commands: Seq[JsCommand.Command])(implicit
      token: Option[String],
      contextualizedErrorLogger: ContextualizedErrorLogger,
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
            template = IdentifierConverter.fromJson(cmd.template_id),
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            jsonArgsValue = cmd.choice_argument,
          )
        contractKey <-
          schemaProcessors.contractArgFromJsonToProto(
            template = IdentifierConverter.fromJson(cmd.template_id),
            jsonArgsValue = cmd.contract_key,
          )
      } yield lapi.commands.Command.Command.ExerciseByKey(
        lapi.commands.ExerciseByKeyCommand(
          templateId = Some(IdentifierConverter.fromJson(cmd.template_id)),
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
              template = IdentifierConverter.fromJson(cmd.template_id),
              jsonArgsValue = cmd.create_arguments,
            )
        choiceArgs <-
          schemaProcessors.choiceArgsFromJsonToProto(
            template = IdentifierConverter.fromJson(cmd.template_id),
            choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
            jsonArgsValue = cmd.choice_argument,
          )
      } yield lapi.commands.Command.Command.CreateAndExercise(
        lapi.commands.CreateAndExerciseCommand(
          templateId = Some(IdentifierConverter.fromJson(cmd.template_id)),
          createArguments = Some(createArgs.getRecord),
          choice = cmd.choice,
          choiceArgument = Some(choiceArgs),
        )
      )
  })

  object Commands extends ProtocolConverter[lapi.commands.Commands, JsCommands] {

    def fromJson(jsCommands: JsCommands)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.commands.Commands] = {
      import jsCommands.*

      val convertedCommands = convertCommands(jsCommands.commands)
      convertedCommands
        .map(cc =>
          lapi.commands.Commands(
            workflowId = workflow_id,
            applicationId = application_id,
            commandId = command_id,
            commands = cc.map(lapi.commands.Command(_)),
            deduplicationPeriod = deduplication_period,
            minLedgerTimeAbs = min_ledger_time_abs,
            minLedgerTimeRel = min_ledger_time_rel,
            actAs = act_as,
            readAs = read_as,
            submissionId = submission_id,
            disclosedContracts = disclosed_contracts,
            synchronizerId = synchronizer_id,
            packageIdSelectionPreference = package_id_selection_preference,
          )
        )
    }

    def toJson(
        lapiCommands: lapi.commands.Commands
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsCommands] = {
      val jsCommands: Seq[Future[JsCommand.Command]] = lapiCommands.commands
        .map(_.command)
        .map {
          case lapi.commands.Command.Command.Empty => jsFail("Invalid value")
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
              template_id = IdentifierConverter.toJson(cmd.getTemplateId),
              create_arguments = createArgs,
              choice = cmd.choice,
              choice_argument = choiceArgs,
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
              template_id = IdentifierConverter.toJson(cmd.getTemplateId),
              contract_key = contractKey,
              choice = cmd.choice,
              choice_argument = choiceArgs,
            )
        }
      Future
        .sequence(jsCommands)
        .map(cmds =>
          JsCommands(
            commands = cmds,
            workflow_id = lapiCommands.workflowId,
            application_id = lapiCommands.applicationId,
            command_id = lapiCommands.commandId,
            deduplication_period = lapiCommands.deduplicationPeriod,
            disclosed_contracts = lapiCommands.disclosedContracts,
            act_as = lapiCommands.actAs,
            read_as = lapiCommands.readAs,
            submission_id = lapiCommands.submissionId,
            synchronizer_id = lapiCommands.synchronizerId,
            min_ledger_time_abs = lapiCommands.minLedgerTimeAbs,
            min_ledger_time_rel = lapiCommands.minLedgerTimeRel,
            package_id_selection_preference = lapiCommands.packageIdSelectionPreference,
          )
        )
    }
  }

  object InterfaceView extends ProtocolConverter[lapi.event.InterfaceView, JsInterfaceView] {

    def fromJson(
        iview: JsInterfaceView
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.event.InterfaceView] = for {
      record <- iview.view_value
        .map { v =>
          schemaProcessors
            .contractArgFromJsonToProto(
              IdentifierConverter.fromJson(iview.interface_id),
              v,
            )
            .map(_.getRecord)
            .map(Some(_))
        }
        .getOrElse(Future.successful(None))
    } yield lapi.event.InterfaceView(
      interfaceId = Some(IdentifierConverter.fromJson(iview.interface_id)),
      viewStatus = Some(JsStatusConverter.fromJson(iview.view_status)),
      viewValue = record,
    )

    def toJson(
        obj: lapi.event.InterfaceView
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsInterfaceView] =
      for {
        record <- schemaProcessors.contractArgFromProtoToJson(
          obj.getInterfaceId,
          obj.getViewValue,
        )
      } yield JsInterfaceView(
        interface_id = IdentifierConverter.toJson(obj.getInterfaceId),
        view_status = JsStatusConverter.toJson(obj.getViewStatus),
        view_value = obj.viewValue.map(_ => record),
      )
  }

  object Event extends ProtocolConverter[lapi.event.Event.Event, JsEvent.Event] {
    def toJson(event: lapi.event.Event.Event)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsEvent.Event] =
      event match {
        case lapi.event.Event.Event.Empty => jsFail("Invalid value")
        case lapi.event.Event.Event.Created(value) =>
          CreatedEvent.toJson(value)
        case lapi.event.Event.Event.Archived(value) =>
          Future(ArchivedEvent.toJson(value))
      }

    def fromJson(event: JsEvent.Event)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.event.Event.Event] = event match {
      case createdEvent: JsEvent.CreatedEvent =>
        CreatedEvent.fromJson(createdEvent).map(lapi.event.Event.Event.Created.apply)
      case archivedEvent: JsEvent.ArchivedEvent =>
        Future.successful(lapi.event.Event.Event.Archived(ArchivedEvent.fromJson(archivedEvent)))
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
        case lapi.topology_transaction.TopologyEvent.Event.Empty => jsFail("Invalid value")
        case lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationChanged(value) =>
          Future(ParticipantAuthorizationChanged.toJson(value))
        case lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationRevoked(value) =>
          Future(ParticipantAuthorizationRevoked.toJson(value))
      }

    def fromJson(
        event: JsTopologyEvent.Event
    ): Future[lapi.topology_transaction.TopologyEvent.Event] = event match {
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsTransaction] =
      Future
        .sequence(v.events.map(e => Event.toJson(e.event)))
        .map(ev =>
          JsTransaction(
            update_id = v.updateId,
            command_id = v.commandId,
            workflow_id = v.workflowId,
            effective_at = v.getEffectiveAt,
            events = ev,
            offset = v.offset,
            synchronizer_id = v.synchronizerId,
            trace_context = v.traceContext,
            record_time = v.getRecordTime,
          )
        )

    def fromJson(v: JsTransaction)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.transaction.Transaction] = Future
      .sequence(v.events.map(e => Event.fromJson(e)))
      .map { ev =>
        lapi.transaction.Transaction(
          updateId = v.update_id,
          commandId = v.command_id,
          workflowId = v.workflow_id,
          effectiveAt = Some(v.effective_at),
          events = ev.map(lapi.event.Event(_)),
          offset = v.offset,
          synchronizerId = v.synchronizer_id,
          traceContext = v.trace_context,
          recordTime = Some(v.record_time),
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
            update_id = v.updateId,
            events = ev,
            offset = v.offset,
            synchronizer_id = v.synchronizerId,
            trace_context = v.traceContext,
            record_time = v.getRecordTime,
          )
        )

    def fromJson(v: JsTopologyTransaction): Future[lapi.topology_transaction.TopologyTransaction] =
      Future
        .sequence(v.events.map(e => TopologyEvent.fromJson(e)))
        .map { ev =>
          lapi.topology_transaction.TopologyTransaction(
            updateId = v.update_id,
            events = ev.map(lapi.topology_transaction.TopologyEvent(_)),
            offset = v.offset,
            synchronizerId = v.synchronizer_id,
            traceContext = v.trace_context,
            recordTime = Some(v.record_time),
          )
        }
  }

  object TransactionTree
      extends ProtocolConverter[lapi.transaction.TransactionTree, JsTransactionTree] {
    def toJson(
        lapiTransactionTree: lapi.transaction.TransactionTree
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsTransactionTree] = {
      val jsEventsById = lapiTransactionTree.eventsById.view
        .mapValues(_.kind)
        .mapValues[Future[JsTreeEvent.TreeEvent]] {
          case lapi.transaction.TreeEvent.Kind.Empty => jsFail("Empty event")
          case lapi.transaction.TreeEvent.Kind.Created(created) =>
            CreatedEvent.toJson(created).map(JsTreeEvent.CreatedTreeEvent(_))
          case lapi.transaction.TreeEvent.Kind.Exercised(exercised) =>
            val apiTemplateId = exercised.getTemplateId

            val choiceName = Ref.ChoiceName.assertFromString(exercised.choice)
            for {
              choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
                template = apiTemplateId,
                choiceName = choiceName,
                protoArgs = exercised.getChoiceArgument,
              )
              exerciseResult <- schemaProcessors.exerciseResultFromProtoToJson(
                apiTemplateId,
                choiceName,
                exercised.getExerciseResult,
              )
            } yield JsTreeEvent.ExercisedTreeEvent(
              event_id = exercised.eventId,
              offset = exercised.offset,
              node_id = exercised.nodeId,
              contract_id = exercised.contractId,
              template_id = IdentifierConverter.toJson(apiTemplateId),
              interface_id = exercised.interfaceId.map(IdentifierConverter.toJson),
              choice = exercised.choice,
              choice_argument = choiceArgs,
              acting_parties = exercised.actingParties,
              consuming = exercised.consuming,
              witness_parties = exercised.witnessParties,
              child_event_ids = exercised.childEventIds,
              exercise_result = exerciseResult,
              package_name = exercised.packageName,
            )
        }
      Future
        .traverse(jsEventsById.toSeq) { case (key, fv) =>
          fv.map(key -> _)
        }
        .map(_.toMap)
        .map(jsEvents =>
          JsTransactionTree(
            update_id = lapiTransactionTree.updateId,
            command_id = lapiTransactionTree.commandId,
            workflow_id = lapiTransactionTree.workflowId,
            effective_at = lapiTransactionTree.effectiveAt,
            offset = lapiTransactionTree.offset,
            events_by_id = jsEvents,
            root_event_ids = lapiTransactionTree.rootEventIds,
            synchronizer_id = lapiTransactionTree.synchronizerId,
            trace_context = lapiTransactionTree.traceContext,
            record_time = lapiTransactionTree.getRecordTime,
          )
        )
    }

    def fromJson(
        jsTransactionTree: JsTransactionTree
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.transaction.TransactionTree] = {
      val lapiEventsById = jsTransactionTree.events_by_id.view
        .mapValues {
          case JsTreeEvent.CreatedTreeEvent(created) =>
            CreatedEvent
              .fromJson(created)
              .map(ev => lapi.transaction.TreeEvent(lapi.transaction.TreeEvent.Kind.Created(ev)))

          case exercised: JsTreeEvent.ExercisedTreeEvent =>
            val apiTemplateId = IdentifierConverter.fromJson(exercised.template_id)
            val choiceName = Ref.ChoiceName.assertFromString(exercised.choice)
            for {
              choiceArgs <- schemaProcessors.choiceArgsFromJsonToProto(
                template = apiTemplateId,
                choiceName = choiceName,
                jsonArgsValue = ujson.read(
                  CirceJson.transform(exercised.choice_argument, StringRenderer()).toString
                ),
              )
              lapiExerciseResult <- schemaProcessors.exerciseResultFromJsonToProto(
                template = apiTemplateId,
                choiceName = choiceName,
                value = ujson.read(
                  CirceJson.transform(exercised.exercise_result, StringRenderer()).toString
                ),
              )
            } yield lapi.transaction.TreeEvent(
              kind = lapi.transaction.TreeEvent.Kind.Exercised(
                lapi.event.ExercisedEvent(
                  eventId = exercised.event_id,
                  offset = exercised.offset,
                  nodeId = exercised.node_id,
                  contractId = exercised.contract_id,
                  templateId = Some(apiTemplateId),
                  interfaceId = exercised.interface_id.map(IdentifierConverter.fromJson),
                  choice = exercised.choice,
                  choiceArgument = Some(choiceArgs),
                  actingParties = exercised.acting_parties,
                  consuming = exercised.consuming,
                  witnessParties = exercised.witness_parties,
                  childEventIds = exercised.child_event_ids,
                  exerciseResult = lapiExerciseResult,
                  packageName = exercised.package_name,
                )
              )
            )
        }
      Future
        .traverse(lapiEventsById.toSeq) { case (key, fv) =>
          fv.map(key -> _)
        }
        .map(events =>
          lapi.transaction.TransactionTree(
            eventsById = events.toMap,
            rootEventIds = jsTransactionTree.root_event_ids,
            offset = jsTransactionTree.offset,
            updateId = jsTransactionTree.update_id,
            commandId = jsTransactionTree.command_id,
            workflowId = jsTransactionTree.workflow_id,
            effectiveAt = jsTransactionTree.effective_at,
            synchronizerId = jsTransactionTree.synchronizer_id,
            traceContext = jsTransactionTree.trace_context,
            recordTime = Some(jsTransactionTree.record_time),
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsSubmitAndWaitForTransactionTreeResponse] =
      TransactionTree
        .toJson(response.getTransaction)
        .map(tree =>
          JsSubmitAndWaitForTransactionTreeResponse(
            transaction_tree = tree
          )
        )

    def fromJson(
        response: JsSubmitAndWaitForTransactionTreeResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.command_service.SubmitAndWaitForTransactionTreeResponse] =
      TransactionTree
        .fromJson(response.transaction_tree)
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.command_service.SubmitAndWaitForTransactionResponse] = Transaction
      .fromJson(jsResponse.transaction)
      .map(tx =>
        lapi.command_service.SubmitAndWaitForTransactionResponse(
          transaction = Some(tx)
        )
      )
  }

  object SubmitAndWaitResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitResponse,
        JsSubmitAndWaitResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitResponse
    ): JsSubmitAndWaitResponse =
      JsSubmitAndWaitResponse(
        update_id = response.updateId,
        completion_offset = response.completionOffset,
      )

    def fromJson(
        response: JsSubmitAndWaitResponse
    ): lapi.command_service.SubmitAndWaitResponse =
      lapi.command_service.SubmitAndWaitResponse(
        updateId = response.update_id,
        completionOffset = response.completion_offset,
      )
  }

  object GetEventsByContractIdRequest
      extends ProtocolConverter[
        lapi.event_query_service.GetEventsByContractIdRequest,
        JsGetEventsByContractIdResponse,
      ] {
    def toJson(
        response: lapi.event_query_service.GetEventsByContractIdResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetEventsByContractIdResponse] =
      for {
        createdEvents <- response.created
          .map(c => CreatedEvent.toJson(c.getCreatedEvent).map(Some(_)))
          .getOrElse(Future(None))
      } yield JsGetEventsByContractIdResponse(
        created = response.created.flatMap(c =>
          createdEvents.map(ce =>
            JsCreated(
              created_event = ce,
              synchronizer_id = c.synchronizerId,
            )
          )
        ),
        archived = response.archived.map(a =>
          JsArchived(
            archived_event = ArchivedEvent.toJson(a.getArchivedEvent),
            synchronizer_id = a.synchronizerId,
          )
        ),
      )

    def fromJson(
        obj: JsGetEventsByContractIdResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.event_query_service.GetEventsByContractIdResponse] = for {
      createdEvents <- obj.created
        .map(c => CreatedEvent.fromJson(c.created_event).map(Some(_)))
        .getOrElse(Future(None))
    } yield lapi.event_query_service.GetEventsByContractIdResponse(
      created = obj.created.flatMap((c: JsCreated) =>
        createdEvents.map(ce => lapi.event_query_service.Created(Some(ce), c.synchronizer_id))
      ),
      archived = obj.archived.map(arch =>
        lapi.event_query_service.Archived(
          archivedEvent = Some(ArchivedEvent.fromJson(arch.archived_event)),
          synchronizerId = arch.synchronizer_id,
        )
      ),
    )
  }

  object ArchivedEvent extends ProtocolConverter[lapi.event.ArchivedEvent, JsEvent.ArchivedEvent] {
    def toJson(e: lapi.event.ArchivedEvent): JsEvent.ArchivedEvent = JsEvent.ArchivedEvent(
      event_id = e.eventId,
      offset = e.offset,
      node_id = e.nodeId,
      contract_id = e.contractId,
      template_id = IdentifierConverter.toJson(e.getTemplateId),
      witness_parties = e.witnessParties,
      package_name = e.packageName,
    )

    def fromJson(ev: JsEvent.ArchivedEvent): lapi.event.ArchivedEvent = lapi.event.ArchivedEvent(
      eventId = ev.event_id,
      offset = ev.offset,
      nodeId = ev.node_id,
      contractId = ev.contract_id,
      templateId = Some(IdentifierConverter.fromJson(ev.template_id)),
      witnessParties = ev.witness_parties,
      packageName = ev.package_name,
    )
  }

  object CreatedEvent extends ProtocolConverter[lapi.event.CreatedEvent, JsEvent.CreatedEvent] {
    def toJson(created: lapi.event.CreatedEvent)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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
        event_id = created.eventId,
        offset = created.offset,
        node_id = created.nodeId,
        contract_id = created.contractId,
        template_id = IdentifierConverter.toJson(created.getTemplateId),
        contract_key = contractKey.map(toCirce),
        create_argument = createdArgs.map(toCirce),
        created_event_blob = created.createdEventBlob,
        interface_views = interfaceViews,
        witness_parties = created.witnessParties,
        signatories = created.signatories,
        observers = created.observers,
        created_at = created.getCreatedAt,
        package_name = created.packageName,
      )

    def fromJson(createdEvent: JsEvent.CreatedEvent)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.event.CreatedEvent] = {
      val templateId = IdentifierConverter.fromJson(createdEvent.template_id)
      for {
        contractKey <- createdEvent.contract_key
          .map(key =>
            schemaProcessors
              .keyArgFromJsonToProto(templateId, key)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        createArgs <- createdEvent.create_argument
          .map(args =>
            schemaProcessors
              .contractArgFromJsonToProto(templateId, args)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        interfaceViews <- Future.sequence(createdEvent.interface_views.map(InterfaceView.fromJson))
      } yield lapi.event.CreatedEvent(
        eventId = createdEvent.event_id,
        offset = createdEvent.offset,
        nodeId = createdEvent.node_id,
        contractId = createdEvent.contract_id,
        templateId = Some(templateId),
        contractKey = contractKey,
        createArguments = createArgs.map(_.getRecord),
        createdEventBlob = createdEvent.created_event_blob,
        interfaceViews = interfaceViews,
        witnessParties = createdEvent.witness_parties,
        signatories = createdEvent.signatories,
        observers = createdEvent.observers,
        createdAt = Some(createdEvent.created_at),
        packageName = createdEvent.package_name,
      )
    }

  }

  object AssignedEvent
      extends ProtocolConverter[
        lapi.reassignment.AssignedEvent,
        JsAssignedEvent,
      ] {

    def toJson(v: lapi.reassignment.AssignedEvent)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsAssignedEvent] =
      CreatedEvent
        .toJson(v.getCreatedEvent)
        .map(ev =>
          JsAssignedEvent(
            source = v.source,
            target = v.target,
            unassign_id = v.unassignId,
            submitter = v.submitter,
            reassignment_counter = v.reassignmentCounter,
            created_event = ev,
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
        party_id = e.partyId,
        participant_id = e.participantId,
        particiant_permission = e.particiantPermission.value,
      )

    def fromJson(
        ev: JsTopologyEvent.ParticipantAuthorizationChanged
    ): lapi.topology_transaction.ParticipantAuthorizationChanged =
      lapi.topology_transaction.ParticipantAuthorizationChanged(
        partyId = ev.party_id,
        participantId = ev.participant_id,
        particiantPermission =
          lapi.state_service.ParticipantPermission.fromValue(ev.particiant_permission),
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
        party_id = e.partyId,
        participant_id = e.participantId,
      )

    def fromJson(
        ev: JsTopologyEvent.ParticipantAuthorizationRevoked
    ): lapi.topology_transaction.ParticipantAuthorizationRevoked =
      lapi.topology_transaction.ParticipantAuthorizationRevoked(
        partyId = ev.party_id,
        participantId = ev.participant_id,
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsContractEntry] =
      v match {
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty =>
          Future(JsContractEntry.JsEmpty)
        case lapi.state_service.GetActiveContractsResponse.ContractEntry.ActiveContract(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsContractEntry.JsActiveContract(
                created_event = ce,
                synchronizer_id = value.synchronizerId,
                reassignment_counter = value.reassignmentCounter,
              )
            )
        case lapi.state_service.GetActiveContractsResponse.ContractEntry
              .IncompleteUnassigned(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsContractEntry.JsIncompleteUnassigned(
                created_event = ce,
                unassigned_event = value.getUnassignedEvent,
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
        contract_entry: JsContractEntry
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.state_service.GetActiveContractsResponse.ContractEntry] = contract_entry match {
      case JsContractEntry.JsEmpty =>
        Future(lapi.state_service.GetActiveContractsResponse.ContractEntry.Empty)
      case JsContractEntry.JsIncompleteAssigned(assigned_event) =>
        CreatedEvent
          .fromJson(assigned_event.created_event)
          .map(ce =>
            lapi.state_service.GetActiveContractsResponse.ContractEntry.IncompleteAssigned(
              new lapi.state_service.IncompleteAssigned(
                Some(
                  lapi.reassignment.AssignedEvent(
                    source = assigned_event.source,
                    target = assigned_event.target,
                    unassignId = assigned_event.unassign_id,
                    submitter = assigned_event.submitter,
                    reassignmentCounter = assigned_event.reassignment_counter,
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetActiveContractsResponse] =
      ContractEntry
        .toJson(v.contractEntry)
        .map(ce =>
          JsGetActiveContractsResponse(
            workflow_id = v.workflowId,
            contract_entry = ce,
          )
        )

    def fromJson(
        v: JsGetActiveContractsResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.state_service.GetActiveContractsResponse] =
      ContractEntry
        .fromJson(v.contract_entry)
        .map(ce =>
          lapi.state_service.GetActiveContractsResponse(
            workflowId = v.workflow_id,
            contractEntry = ce,
          )
        )
  }

  object ReassignmentEvent
      extends ProtocolConverter[lapi.reassignment.Reassignment.Event, JsReassignmentEvent] {
    def toJson(v: lapi.reassignment.Reassignment.Event)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsReassignmentEvent] =
      v match {
        case lapi.reassignment.Reassignment.Event.Empty => jsFail("Invalid value")
        case lapi.reassignment.Reassignment.Event.UnassignedEvent(value) =>
          Future(JsReassignmentEvent.JsUnassignedEvent(value))
        case lapi.reassignment.Reassignment.Event.AssignedEvent(value) =>
          CreatedEvent
            .toJson(value.getCreatedEvent)
            .map(ce =>
              JsReassignmentEvent.JsAssignmentEvent(
                source = value.source,
                target = value.target,
                unassign_id = value.unassignId,
                submitter = value.submitter,
                reassignment_counter = value.reassignmentCounter,
                created_event = ce,
              )
            )
      }

    def fromJson(jsObj: JsReassignmentEvent)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.reassignment.Reassignment.Event] =
      jsObj match {
        case event: JsReassignmentEvent.JsAssignmentEvent =>
          CreatedEvent
            .fromJson(event.created_event)
            .map(ce =>
              lapi.reassignment.Reassignment.Event.AssignedEvent(
                value = lapi.reassignment.AssignedEvent(
                  source = event.source,
                  target = event.target,
                  unassignId = event.unassign_id,
                  submitter = event.submitter,
                  reassignmentCounter = event.reassignment_counter,
                  createdEvent = Some(ce),
                )
              )
            )

        case JsReassignmentEvent.JsUnassignedEvent(value) =>
          Future.successful(lapi.reassignment.Reassignment.Event.UnassignedEvent(value))
      }

  }

  object Reassignment extends ProtocolConverter[lapi.reassignment.Reassignment, JsReassignment] {
    def toJson(v: lapi.reassignment.Reassignment)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsReassignment] = ReassignmentEvent
      .toJson(v.event)
      .map(e =>
        JsReassignment(
          update_id = v.updateId,
          command_id = v.commandId,
          workflow_id = v.workflowId,
          offset = v.offset,
          event = e,
          trace_context = v.traceContext,
          record_time = v.getRecordTime,
        )
      )

    def fromJson(value: JsReassignment)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.reassignment.Reassignment] =
      ReassignmentEvent
        .fromJson(value.event)
        .map(re =>
          lapi.reassignment.Reassignment(
            updateId = value.update_id,
            commandId = value.command_id,
            workflowId = value.workflow_id,
            offset = value.offset,
            event = re,
            traceContext = value.trace_context,
            recordTime = Some(value.record_time),
          )
        )
  }

  object GetUpdatesResponse
      extends ProtocolConverter[lapi.update_service.GetUpdatesResponse, JsGetUpdatesResponse] {
    def toJson(obj: lapi.update_service.GetUpdatesResponse)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetUpdatesResponse] =
      ((obj.update match {
        case lapi.update_service.GetUpdatesResponse.Update.Empty => jsFail("Invalid value")
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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

  object GetUpdateTreesResponse
      extends ProtocolConverter[
        lapi.update_service.GetUpdateTreesResponse,
        JsGetUpdateTreesResponse,
      ] {
    def toJson(
        value: lapi.update_service.GetUpdateTreesResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetUpdateTreesResponse] =
      ((value.update match {
        case lapi.update_service.GetUpdateTreesResponse.Update.Empty => jsFail("Invalid value")
        case lapi.update_service.GetUpdateTreesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdateTree.OffsetCheckpoint(value))
        case lapi.update_service.GetUpdateTreesResponse.Update.TransactionTree(value) =>
          TransactionTree.toJson(value).map(JsUpdateTree.TransactionTree.apply)
        case lapi.update_service.GetUpdateTreesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdateTree.Reassignment.apply)
        case lapi.update_service.GetUpdateTreesResponse.Update.TopologyTransaction(value) =>
          TopologyTransaction.toJson(value).map(JsUpdateTree.TopologyTransaction.apply)
      }): Future[JsUpdateTree.Update]).map(update => JsGetUpdateTreesResponse(update))

    def fromJson(
        jsObj: JsGetUpdateTreesResponse
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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
        case JsUpdateTree.TopologyTransaction(value) =>
          TopologyTransaction
            .fromJson(value)
            .map(lapi.update_service.GetUpdateTreesResponse.Update.TopologyTransaction.apply)
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetTransactionTreeResponse] =
      TransactionTree.toJson(obj.getTransaction).map(JsGetTransactionTreeResponse.apply)

    def fromJson(treeResponse: JsGetTransactionTreeResponse)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[JsGetTransactionResponse] =
      Transaction.toJson(obj.getTransaction).map(JsGetTransactionResponse.apply)

    def fromJson(obj: JsGetTransactionResponse)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
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
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.interactive.interactive_submission_service.PrepareSubmissionRequest] = for {
      commands <- convertCommands(obj.commands)
    } yield lapi.interactive.interactive_submission_service.PrepareSubmissionRequest(
      applicationId = obj.application_id,
      commandId = obj.command_id,
      commands = commands.map(lapi.commands.Command(_)),
      minLedgerTime = obj.min_ledger_time,
      actAs = obj.act_as,
      readAs = obj.read_as,
      disclosedContracts = obj.disclosed_contracts,
      synchronizerId = obj.synchronizer_id,
      packageIdSelectionPreference = obj.package_id_selection_preference,
      verboseHashing = obj.verbose_hashing,
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
        prepared_transaction = obj.preparedTransaction.map(_.toByteString),
        prepared_transaction_hash = obj.preparedTransactionHash,
        hashing_scheme_version = obj.hashingSchemeVersion,
        hashing_details = obj.hashingDetails,
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
        val preparedTransaction = obj.prepared_transaction.map { proto =>
          ProtoConverter
            .protoParser(
              lapi.interactive.interactive_submission_service.PreparedTransaction.parseFrom
            )(proto)
            .getOrElse(jsFail("Cannot parse prepared_transaction"))
        }
        lapi.interactive.interactive_submission_service.ExecuteSubmissionRequest(
          preparedTransaction = preparedTransaction,
          partySignatures = obj.party_signatures,
          deduplicationPeriod = obj.deduplication_period,
          submissionId = obj.submission_id,
          applicationId = obj.application_id,
          hashingSchemeVersion = obj.hashing_scheme_version,
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
      case _ => jsFail(s"Invalid identifier: $jsIdentifier")
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
