// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.ledger.api.refinements.{ApiTypes => lar}
import com.daml.ledger.api.{v1 => lav1}
import scalaz.syntax.tag._

object Commands {
  def create(
      templateId: lar.TemplateId,
      payload: lav1.value.Record
  ): lav1.commands.Command.Command.Create =
    lav1.commands.Command.Command.Create(
      lav1.commands
        .CreateCommand(templateId = Some(templateId.unwrap), createArguments = Some(payload)))

  def exercise(
      templateId: lar.TemplateId,
      contractId: lar.ContractId,
      choice: lar.Choice,
      argument: lav1.value.Value
  ): lav1.commands.Command.Command.Exercise =
    lav1.commands.Command.Command.Exercise(
      lav1.commands.ExerciseCommand(
        templateId = Some(templateId.unwrap),
        contractId = contractId.unwrap,
        choice = choice.unwrap,
        choiceArgument = Some(argument)
      )
    )

  def exerciseByKey(
      templateId: lar.TemplateId,
      contractKey: lav1.value.Value,
      choice: lar.Choice,
      argument: lav1.value.Value
  ): lav1.commands.Command.Command.ExerciseByKey =
    lav1.commands.Command.Command.ExerciseByKey(
      lav1.commands.ExerciseByKeyCommand(
        templateId = Some(templateId.unwrap),
        contractKey = Some(contractKey),
        choice = choice.unwrap,
        choiceArgument = Some(argument)
      )
    )

  def createAndExercise(
      templateId: lar.TemplateId,
      payload: lav1.value.Record,
      choice: lar.Choice,
      argument: lav1.value.Value
  ): lav1.commands.Command.Command.CreateAndExercise =
    lav1.commands.Command.Command.CreateAndExercise(
      lav1.commands.CreateAndExerciseCommand(
        templateId = Some(templateId.unwrap),
        createArguments = Some(payload),
        choice = choice.unwrap,
        choiceArgument = Some(argument)
      )
    )

  def submitAndWaitRequest(
      ledgerId: lar.LedgerId,
      applicationId: lar.ApplicationId,
      commandId: lar.CommandId,
      party: lar.Party,
      command: lav1.commands.Command.Command
  ): lav1.command_service.SubmitAndWaitRequest = {
    val commands = lav1.commands.Commands(
      ledgerId = ledgerId.unwrap,
      applicationId = applicationId.unwrap,
      commandId = commandId.unwrap,
      party = party.unwrap,
      commands = Seq(lav1.commands.Command(command))
    )
    lav1.command_service.SubmitAndWaitRequest(Some(commands))
  }
}
