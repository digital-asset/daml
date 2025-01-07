// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.ledger.api.v2 as lav2
import com.digitalasset.canton.http.domain
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import scalaz.NonEmptyList
import scalaz.syntax.foldable.*
import scalaz.syntax.tag.*

import lav2.commands.Commands.DeduplicationPeriod

object Commands {
  def create(
      templateId: lar.TemplateId,
      payload: lav2.value.Record,
  ): lav2.commands.Command.Command.Create =
    lav2.commands.Command.Command.Create(
      lav2.commands
        .CreateCommand(templateId = Some(templateId.unwrap), createArguments = Some(payload))
    )

  def exercise(
      templateId: lar.TemplateId,
      contractId: lar.ContractId,
      choice: lar.Choice,
      argument: lav2.value.Value,
  ): lav2.commands.Command.Command.Exercise =
    lav2.commands.Command.Command.Exercise(
      lav2.commands.ExerciseCommand(
        templateId = Some(templateId.unwrap),
        contractId = contractId.unwrap,
        choice = choice.unwrap,
        choiceArgument = Some(argument),
      )
    )

  def exerciseByKey(
      templateId: lar.TemplateId,
      contractKey: lav2.value.Value,
      choice: lar.Choice,
      argument: lav2.value.Value,
  ): lav2.commands.Command.Command.ExerciseByKey =
    lav2.commands.Command.Command.ExerciseByKey(
      lav2.commands.ExerciseByKeyCommand(
        templateId = Some(templateId.unwrap),
        contractKey = Some(contractKey),
        choice = choice.unwrap,
        choiceArgument = Some(argument),
      )
    )

  def createAndExercise(
      templateId: lar.TemplateId,
      payload: lav2.value.Record,
      choice: lar.Choice,
      argument: lav2.value.Value,
  ): lav2.commands.Command.Command.CreateAndExercise =
    lav2.commands.Command.Command.CreateAndExercise(
      lav2.commands.CreateAndExerciseCommand(
        templateId = Some(templateId.unwrap),
        createArguments = Some(payload),
        choice = choice.unwrap,
        choiceArgument = Some(argument),
      )
    )

  def submitAndWaitRequest(
      applicationId: lar.ApplicationId,
      commandId: lar.CommandId,
      actAs: NonEmptyList[lar.Party],
      readAs: List[lar.Party],
      command: lav2.commands.Command.Command,
      deduplicationPeriod: DeduplicationPeriod,
      submissionId: Option[domain.SubmissionId],
      workflowId: Option[domain.WorkflowId],
      disclosedContracts: Seq[domain.DisclosedContract.LAV],
      synchronizerId: Option[SynchronizerId],
      packageIdSelectionPreference: Seq[Ref.PackageId],
  ): lav2.command_service.SubmitAndWaitRequest = {
    val commands = lav2.commands.Commands(
      applicationId = applicationId.unwrap,
      commandId = commandId.unwrap,
      actAs = lar.Party.unsubst(actAs.toList),
      readAs = lar.Party.unsubst(readAs),
      deduplicationPeriod = deduplicationPeriod,
      disclosedContracts = disclosedContracts map (_.toLedgerApi),
      synchronizerId = synchronizerId.map(_.toProtoPrimitive).getOrElse(""),
      packageIdSelectionPreference = packageIdSelectionPreference,
      commands = Seq(lav2.commands.Command(command)),
    )
    val commandsWithSubmissionId =
      domain.SubmissionId.unsubst(submissionId).map(commands.withSubmissionId).getOrElse(commands)
    val commandsWithWorkflowId =
      domain.WorkflowId
        .unsubst(workflowId)
        .map(commandsWithSubmissionId.withWorkflowId)
        .getOrElse(commandsWithSubmissionId)
    lav2.command_service.SubmitAndWaitRequest(Some(commandsWithWorkflowId))
  }
}
