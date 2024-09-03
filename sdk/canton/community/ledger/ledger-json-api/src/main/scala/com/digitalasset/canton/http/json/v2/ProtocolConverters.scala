// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2 as lapi
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.fetchcontracts.util.IdentifierConverters
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsContractEntry
import com.digitalasset.canton.http.json.v2.JsReassignmentEvent.JsReassignmentEvent
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsEvent,
  JsInterfaceView,
  JsStatus,
  JsTransaction,
  JsTransactionTree,
  JsTreeEvent,
}
import com.google.rpc.status.Status
import io.circe.Json
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

  object JsReassignmentCommandConverter
      extends ProtocolConverter[
        lapi.reassignment_command.ReassignmentCommand.Command,
        JsReassignmentCommand.JsCommand,
      ] {
    def fromJson(
        command: JsReassignmentCommand.JsCommand
    ): lapi.reassignment_command.ReassignmentCommand.Command =
      command match {
        case cmd: JsReassignmentCommand.JsUnassignCommand =>
          lapi.reassignment_command.ReassignmentCommand.Command.UnassignCommand(
            lapi.reassignment_command.UnassignCommand(
              contractId = cmd.contract_id,
              source = cmd.source,
              target = cmd.target,
            )
          )
        case cmd: JsReassignmentCommand.JsAssignCommand =>
          lapi.reassignment_command.ReassignmentCommand.Command.AssignCommand(
            lapi.reassignment_command.AssignCommand(
              unassignId = cmd.unassign_id,
              source = cmd.source,
              target = cmd.target,
            )
          )
      }

  }

  object JsSubmitReassignmentRequest
      extends ProtocolConverter[
        lapi.command_submission_service.SubmitReassignmentRequest,
        JsSubmitReassignmentRequest,
      ] {
    def fromJson(
        jsSubmission: JsSubmitReassignmentRequest
    ): lapi.command_submission_service.SubmitReassignmentRequest =
      lapi.command_submission_service.SubmitReassignmentRequest(reassignmentCommand =
        jsSubmission.reassignment_command.map { reassignmentCommands =>
          com.daml.ledger.api.v2.reassignment_command.ReassignmentCommand(
            workflowId = reassignmentCommands.workflow_id,
            applicationId = reassignmentCommands.application_id,
            commandId = reassignmentCommands.command_id,
            submitter = reassignmentCommands.submitter,
            command = JsReassignmentCommandConverter.fromJson(reassignmentCommands.command),
            submissionId = reassignmentCommands.submission_id,
          )
        }
      )
  }

  object Commands extends ProtocolConverter[lapi.commands.Commands, JsCommands] {

    private def optionToJson(v: Option[ujson.Value]): Json = {
      val opt: ujson.Value = v.getOrElse(ujson.Null)
      opt
    }

    def fromJson(jsCommands: JsCommands)(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.commands.Commands] = {
      import jsCommands.*

      val convertedCommands: Seq[Future[lapi.commands.Command.Command]] = commands.map {
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
      }
      Future
        .sequence(convertedCommands)
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
            disclosedContracts = disclosed_contracts.map(js =>
              lapi.commands.DisclosedContract(
                templateId = Some(IdentifierConverter.fromJson(js.template_id)),
                contractId = js.contract_id,
                createdEventBlob = js.created_event_blob,
              )
            ),
            domainId = domain_id,
            packageIdSelectionPreference = package_id_selection_preference,
          )
        )
    }

    def toJson(lapiCommands: lapi.commands.Commands)(token: Option[String]): Future[JsCommands] = {
      val jsCommands: Seq[Future[JsCommand.Command]] = lapiCommands.commands
        .map(_.command)
        .map {
          case lapi.commands.Command.Command.Empty => jsFail("Invalid value")
          case lapi.commands.Command.Command.Create(createCommand) =>
            for {
              contractArgs <- schemaProcessors.contractArgFromProtoToJson(
                IdentifierConverters.lfIdentifier(createCommand.getTemplateId),
                createCommand.getCreateArguments,
              )(token)
            } yield JsCommand.CreateCommand(
              IdentifierConverter.toJson(createCommand.getTemplateId),
              contractArgs,
            )

          case lapi.commands.Command.Command.Exercise(exerciseCommand) =>
            for {
              choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
                templateId = IdentifierConverters.lfIdentifier(exerciseCommand.getTemplateId),
                choiceName = Ref.ChoiceName.assertFromString(exerciseCommand.choice),
                protoArgs = exerciseCommand.getChoiceArgument,
              )(token)
            } yield JsCommand.ExerciseCommand(
              IdentifierConverter.toJson(exerciseCommand.getTemplateId),
              exerciseCommand.contractId,
              exerciseCommand.choice,
              choiceArgs,
            )

          case lapi.commands.Command.Command.CreateAndExercise(cmd) =>
            for {
              createArgs <- schemaProcessors.contractArgFromProtoToJson(
                templateId = IdentifierConverters.lfIdentifier(cmd.getTemplateId),
                protoArgs = cmd.getCreateArguments,
              )(token)
              choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
                templateId = IdentifierConverters.lfIdentifier(cmd.getTemplateId),
                choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
                protoArgs = cmd.getChoiceArgument,
              )(token)
            } yield JsCommand.CreateAndExerciseCommand(
              template_id = IdentifierConverter.toJson(cmd.getTemplateId),
              create_arguments = createArgs,
              choice = cmd.choice,
              choice_argument = choiceArgs,
            )
          case lapi.commands.Command.Command.ExerciseByKey(cmd) =>
            for {
              contractKey <- schemaProcessors.keyArgFromProtoToJson(
                templateId = IdentifierConverters.lfIdentifier(cmd.getTemplateId),
                protoArgs = cmd.getContractKey,
              )(token)
              choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
                templateId = IdentifierConverters.lfIdentifier(cmd.getTemplateId),
                choiceName = Ref.ChoiceName.assertFromString(cmd.choice),
                protoArgs = cmd.getChoiceArgument,
              )(token)
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
            disclosed_contracts = lapiCommands.disclosedContracts.map { disclosedContract =>
              JsDisclosedContract(
                template_id = IdentifierConverter.toJson(disclosedContract.getTemplateId),
                contract_id = disclosedContract.contractId,
                created_event_blob = disclosedContract.createdEventBlob,
              )
            },
            act_as = lapiCommands.actAs,
            read_as = lapiCommands.readAs,
            submission_id = lapiCommands.submissionId,
            domain_id = lapiCommands.domainId,
            min_ledger_time_abs = lapiCommands.minLedgerTimeAbs,
            min_ledger_time_rel = lapiCommands.minLedgerTimeRel,
            package_id_selection_preference = lapiCommands.packageIdSelectionPreference,
          )
        )
    }
  }

  object InterfaceView
      extends ProtocolConverter[com.daml.ledger.api.v2.event.InterfaceView, JsInterfaceView] {

    def fromJson(
        iview: JsInterfaceView
    )(implicit
        token: Option[String],
        contextualizedErrorLogger: ContextualizedErrorLogger,
    ): Future[lapi.event.InterfaceView] = for {
      record <- schemaProcessors.contractArgFromJsonToProto(
        IdentifierConverter.fromJson(iview.interface_id),
        iview.view_value,
      )
    } yield lapi.event.InterfaceView(
      interfaceId = Some(IdentifierConverter.fromJson(iview.interface_id)),
      viewStatus = Some(JsStatusConverter.fromJson(iview.view_status)),
      viewValue = iview.view_value.map(_ => record.getRecord),
    )

    def toJson(
        iview: com.daml.ledger.api.v2.event.InterfaceView
    )(implicit token: Option[String]): Future[JsInterfaceView] =
      for {
        record <- schemaProcessors.contractArgFromProtoToJson(
          IdentifierConverters.lfIdentifier(
            iview.getInterfaceId
          ),
          iview.getViewValue,
        )(token)
      } yield JsInterfaceView(
        interface_id = IdentifierConverter.toJson(iview.getInterfaceId),
        view_status = JsStatusConverter.toJson(iview.getViewStatus),
        view_value = iview.viewValue.map(_ => record),
      )
  }

  object Event extends ProtocolConverter[com.daml.ledger.api.v2.event.Event.Event, JsEvent.Event] {
    def toJson(event: com.daml.ledger.api.v2.event.Event.Event)(implicit
        token: Option[String]
    ): Future[JsEvent.Event] =
      event match {
        case lapi.event.Event.Event.Empty => jsFail("Invalid value")
        case lapi.event.Event.Event.Created(value) =>
          CreatedEvent.toJson(value)
        case lapi.event.Event.Event.Archived(value) =>
          Future(ArchivedEvent.toJson(value))
      }

  }

  object Transaction extends ProtocolConverter[lapi.transaction.Transaction, JsTransaction] {

    def toJson(v: lapi.transaction.Transaction)(implicit
        token: Option[String]
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
            domain_id = v.domainId,
            trace_context = v.traceContext,
            record_time = v.getRecordTime,
          )
        )
  }

  object TransactionTree
      extends ProtocolConverter[lapi.transaction.TransactionTree, JsTransactionTree] {
    def toJson(
        lapiTransactionTree: lapi.transaction.TransactionTree
    )(implicit
        token: Option[String]
    ): Future[JsTransactionTree] = {
      val jsEventsById = lapiTransactionTree.eventsById.view
        .mapValues(_.kind)
        .mapValues {
          case lapi.transaction.TreeEvent.Kind.Empty => jsFail("Empty event")
          case lapi.transaction.TreeEvent.Kind.Created(created) =>
            val apiTemplateId = created.getTemplateId
            val lfIdentifier = Ref.Identifier.assertFromString(
              s"${apiTemplateId.packageId}:${apiTemplateId.moduleName}:${apiTemplateId.entityName}"
            )
            for {
              contractArgs <- created.createArguments
                .map(args =>
                  schemaProcessors
                    .contractArgFromProtoToJson(
                      lfIdentifier,
                      args,
                    )(token)
                    .map(Some(_))
                )
                .getOrElse(Future(None))
              contractKeys <- created.contractKey
                .map(args =>
                  schemaProcessors
                    .keyArgFromProtoToJson(
                      lfIdentifier,
                      args,
                    )(token)
                    .map(Some(_))
                )
                .getOrElse(Future(None))
              interfaceViews <- Future.sequence(created.interfaceViews.map(InterfaceView.toJson))
            } yield JsTreeEvent.CreatedTreeEvent(
              event_id = created.eventId,
              contract_id = created.contractId,
              template_id = IdentifierConverter.toJson(apiTemplateId),
              contract_key = contractKeys.map(toCirce),
              create_arguments = contractArgs.map(toCirce),
              created_event_blob = created.createdEventBlob,
              interface_views = interfaceViews,
              witness_parties = created.witnessParties,
              signatories = created.signatories,
              observers = created.observers,
              createdAt = created.createdAt,
              packageName = created.packageName,
            )
          case lapi.transaction.TreeEvent.Kind.Exercised(exercised) =>
            val apiTemplateId = exercised.getTemplateId
            val lfIdentifier = Ref.Identifier.assertFromString(
              s"${apiTemplateId.packageId}:${apiTemplateId.moduleName}:${apiTemplateId.entityName}"
            )
            val choiceName = Ref.ChoiceName.assertFromString(exercised.choice)
            for {
              choiceArgs <- schemaProcessors.choiceArgsFromProtoToJson(
                templateId = lfIdentifier,
                choiceName = choiceName,
                protoArgs = exercised.getChoiceArgument,
              )
              exerciseResult <- schemaProcessors.exerciseResultFromProtoToJson(
                lfIdentifier,
                choiceName,
                exercised.getExerciseResult,
              )
            } yield JsTreeEvent.ExercisedTreeEvent(
              event_id = exercised.eventId,
              contract_id = exercised.contractId,
              template_id = IdentifierConverter.toJson(exercised.getTemplateId),
              interface_id = exercised.interfaceId.map(IdentifierConverter.toJson).getOrElse(null),
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
            domain_id = lapiTransactionTree.domainId,
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
          case treeEvent: JsTreeEvent.CreatedTreeEvent =>
            val apiTemplateId = IdentifierConverter.fromJson(treeEvent.template_id)
            for {
              protoCreateArgsRecord <- treeEvent.create_arguments
                .map(args =>
                  schemaProcessors
                    .contractArgFromJsonToProto(
                      template = apiTemplateId,
                      jsonArgsValue = ujson.read(
                        CirceJson.transform(args, StringRenderer()).toString
                      ),
                    )
                    .map(Some(_))
                )
                .getOrElse(Future(None))
              protoCreateKey <- treeEvent.contract_key
                .map(key =>
                  schemaProcessors
                    .keyArgFromJsonToProto(
                      template = apiTemplateId,
                      protoArgs = ujson.read(
                        CirceJson.transform(key, StringRenderer()).toString
                      ),
                    )
                    .map(Some(_))
                )
                .getOrElse(Future(None))
              interfaceViews <- Future.sequence(
                treeEvent.interface_views.map(InterfaceView.fromJson)
              )
            } yield lapi.transaction.TreeEvent(
              kind = lapi.transaction.TreeEvent.Kind.Created(
                lapi.event.CreatedEvent(
                  eventId = treeEvent.event_id,
                  contractId = treeEvent.contract_id,
                  templateId = Some(apiTemplateId),
                  contractKey = protoCreateKey,
                  createArguments = protoCreateArgsRecord.map(_.getRecord),
                  createdEventBlob = treeEvent.created_event_blob,
                  interfaceViews = interfaceViews,
                  witnessParties = treeEvent.witness_parties,
                  signatories = treeEvent.signatories,
                  observers = treeEvent.observers,
                  createdAt = treeEvent.createdAt,
                  packageName = treeEvent.packageName,
                )
              )
            )
          case treeEvent: JsTreeEvent.ExercisedTreeEvent =>
            val apiTemplateId = IdentifierConverter.fromJson(treeEvent.template_id)
            val choiceName = Ref.ChoiceName.assertFromString(treeEvent.choice)
            for {
              choiceArgs <- schemaProcessors.choiceArgsFromJsonToProto(
                template = apiTemplateId,
                choiceName = choiceName,
                jsonArgsValue = ujson.read(
                  CirceJson.transform(treeEvent.choice_argument, StringRenderer()).toString
                ),
              )
              lapiExerciseResult <- schemaProcessors.exerciseResultFromJsonToProto(
                template = apiTemplateId,
                choiceName = choiceName,
                ujson.read(
                  CirceJson.transform(treeEvent.exercise_result, StringRenderer()).toString
                ),
              )
            } yield lapi.transaction.TreeEvent(
              kind = lapi.transaction.TreeEvent.Kind.Exercised(
                lapi.event.ExercisedEvent(
                  eventId = treeEvent.event_id,
                  contractId = treeEvent.contract_id,
                  templateId = Some(apiTemplateId),
                  choice = choiceName,
                  interfaceId = (Option(treeEvent.interface_id)).map(IdentifierConverter.fromJson),
                  choiceArgument = Some(choiceArgs),
                  exerciseResult = lapiExerciseResult,
                  actingParties = treeEvent.acting_parties,
                  consuming = treeEvent.consuming,
                  witnessParties = treeEvent.witness_parties,
                  childEventIds = treeEvent.child_event_ids,
                  packageName = treeEvent.package_name,
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
            domainId = jsTransactionTree.domain_id,
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
    )(implicit token: Option[String]): Future[JsSubmitAndWaitForTransactionTreeResponse] =
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
        token: Option[String]
    ): Future[JsSubmitAndWaitForTransactionResponse] =
      Transaction
        .toJson(response.getTransaction)
        .map(tx =>
          JsSubmitAndWaitForTransactionResponse(
            transaction = tx
          )
        )
  }

  object SubmitAndWaitUpdateIdResponse
      extends ProtocolConverter[
        lapi.command_service.SubmitAndWaitForUpdateIdResponse,
        JsSubmitAndWaitForUpdateIdResponse,
      ] {

    def toJson(
        response: lapi.command_service.SubmitAndWaitForUpdateIdResponse
    ): JsSubmitAndWaitForUpdateIdResponse =
      JsSubmitAndWaitForUpdateIdResponse(
        update_id = response.updateId,
        completion_offset = response.completionOffset,
      )

    def fromJson(
        response: JsSubmitAndWaitForUpdateIdResponse
    ): lapi.command_service.SubmitAndWaitForUpdateIdResponse =
      lapi.command_service.SubmitAndWaitForUpdateIdResponse(
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
        token: Option[String]
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
              domain_id = c.domainId,
            )
          )
        ),
        archived = response.archived.map(a =>
          JsArchived(
            archived_event = ArchivedEvent.toJson(a.getArchivedEvent),
            domain_id = a.domainId,
          )
        ),
      )
  }

  object ArchivedEvent extends ProtocolConverter[lapi.event.ArchivedEvent, JsEvent.ArchivedEvent] {
    def toJson(e: lapi.event.ArchivedEvent): JsEvent.ArchivedEvent = JsEvent.ArchivedEvent(
      event_id = e.eventId,
      contract_id = e.contractId,
      template_id = IdentifierConverter.toJson(e.getTemplateId),
      witness_parties = e.witnessParties,
      package_name = e.packageName,
    )

    def fromJson(ev: JsEvent.ArchivedEvent): lapi.event.ArchivedEvent = lapi.event.ArchivedEvent(
      eventId = ev.event_id,
      contractId = ev.contract_id,
      templateId = Some(IdentifierConverter.fromJson(ev.template_id)),
      witnessParties = ev.witness_parties,
      packageName = ev.package_name,
    )
  }

  object CreatedEvent extends ProtocolConverter[lapi.event.CreatedEvent, JsEvent.CreatedEvent] {
    def toJson(created: lapi.event.CreatedEvent)(implicit
        token: Option[String]
    ): Future[JsEvent.CreatedEvent] = {
      val apiTemplateId = created.getTemplateId
      val lfIdentifier = Ref.Identifier.assertFromString(
        s"${apiTemplateId.packageId}:${apiTemplateId.moduleName}:${apiTemplateId.entityName}"
      )
      for {
        contractKey <- created.contractKey
          .map(ck =>
            schemaProcessors
              .keyArgFromProtoToJson(
                lfIdentifier,
                ck,
              )(token)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        createdArgs <- created.createArguments
          .map(ca =>
            schemaProcessors
              .contractArgFromProtoToJson(
                lfIdentifier,
                ca,
              )(token)
              .map(Some(_))
          )
          .getOrElse(Future(None))
        interfaceViews <- Future.sequence(created.interfaceViews.map(InterfaceView.toJson))
      } yield JsEvent.CreatedEvent(
        event_id = created.eventId,
        contract_id = created.contractId,
        template_id = IdentifierConverter.toJson(created.getTemplateId),
        contract_key = contractKey.map(toCirce(_)),
        create_argument = createdArgs.map(toCirce(_)),
        created_event_blob = created.createdEventBlob,
        interface_views = interfaceViews,
        witness_parties = created.witnessParties,
        signatories = created.signatories,
        observers = created.observers,
        created_at = created.getCreatedAt,
        package_name = created.packageName,
      )
    }

  }

  object AssignedEvent
      extends ProtocolConverter[
        lapi.reassignment.AssignedEvent,
        JsAssignedEvent,
      ] {

    def toJson(v: lapi.reassignment.AssignedEvent)(implicit
        token: Option[String]
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

  object UnassignedEvent
      extends ProtocolConverter[
        lapi.reassignment.UnassignedEvent,
        JsUnassignedEvent,
      ] {
    def toJson(v: lapi.reassignment.UnassignedEvent): JsUnassignedEvent =
      JsUnassignedEvent(
        unassign_id = v.unassignId,
        contract_id = v.contractId,
        template_id = IdentifierConverter.toJson(v.getTemplateId),
        source = v.source,
        target = v.target,
        submitter = v.submitter,
        reassignment_counter = v.reassignmentCounter,
        assignment_exclusivity = v.assignmentExclusivity,
        witness_parties = v.witnessParties,
        package_name = v.packageName,
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
        token: Option[String]
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
                domain_id = value.domainId,
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
                unassigned_event = UnassignedEvent.toJson(value.getUnassignedEvent),
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

  }

  object GetActiveContractsResponse
      extends ProtocolConverter[
        lapi.state_service.GetActiveContractsResponse,
        JsGetActiveContractsResponse,
      ] {
    def toJson(v: lapi.state_service.GetActiveContractsResponse)(implicit
        token: Option[String]
    ): Future[JsGetActiveContractsResponse] =
      ContractEntry
        .toJson(v.contractEntry)
        .map(ce =>
          JsGetActiveContractsResponse(
            offset = v.offset,
            workflow_id = v.workflowId,
            contract_entry = ce,
          )
        )
  }

  object ReassignmentEvent
      extends ProtocolConverter[lapi.reassignment.Reassignment.Event, JsReassignmentEvent] {
    def toJson(v: lapi.reassignment.Reassignment.Event)(implicit
        token: Option[String]
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

  }

  object Reassignment extends ProtocolConverter[lapi.reassignment.Reassignment, JsReassignment] {
    def toJson(v: lapi.reassignment.Reassignment)(implicit
        token: Option[String]
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
  }

  object GetUpdatesResponse
      extends ProtocolConverter[lapi.update_service.GetUpdatesResponse, JsGetUpdatesResponse] {
    def toJson(obj: lapi.update_service.GetUpdatesResponse)(implicit
        token: Option[String]
    ): Future[JsGetUpdatesResponse] =
      (obj.update match {
        case lapi.update_service.GetUpdatesResponse.Update.Empty => jsFail("Invalid value")
        case lapi.update_service.GetUpdatesResponse.Update.Transaction(value) =>
          Transaction.toJson(value).map(JsUpdate.Transaction)
        case lapi.update_service.GetUpdatesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdate.Reassignment)
        case lapi.update_service.GetUpdatesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdate.OffsetCheckpoint(value))
      }).map(update => JsGetUpdatesResponse(update))
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
    ): Future[JsGetUpdateTreesResponse] =
      (value.update match {
        case lapi.update_service.GetUpdateTreesResponse.Update.Empty => jsFail("Invalid value")
        case lapi.update_service.GetUpdateTreesResponse.Update.OffsetCheckpoint(value) =>
          Future(JsUpdateTree.OffsetCheckpoint(value))
        case lapi.update_service.GetUpdateTreesResponse.Update.TransactionTree(value) =>
          TransactionTree.toJson(value).map(JsUpdateTree.TransactionTree)
        case lapi.update_service.GetUpdateTreesResponse.Update.Reassignment(value) =>
          Reassignment.toJson(value).map(JsUpdateTree.Reassignment)
      }).map(update => JsGetUpdateTreesResponse(update))
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
    ): Future[JsGetTransactionTreeResponse] =
      TransactionTree.toJson(obj.getTransaction).map(JsGetTransactionTreeResponse)
  }

  object GetTransactionResponse
      extends ProtocolConverter[
        lapi.update_service.GetTransactionResponse,
        JsGetTransactionResponse,
      ] {
    def toJson(obj: lapi.update_service.GetTransactionResponse)(implicit
        token: Option[String]
    ): Future[JsGetTransactionResponse] =
      Transaction.toJson(obj.getTransaction).map(JsGetTransactionResponse)
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
