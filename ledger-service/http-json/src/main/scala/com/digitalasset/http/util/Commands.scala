// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import java.time.Instant

import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}

object Commands {
  def create(
      templateId: lar.TemplateId,
      arguments: lav1.value.Record): lav1.commands.Command.Command.Create =
    lav1.commands.Command.Command.Create(
      lav1.commands.CreateCommand(
        templateId = Some(lar.TemplateId.unwrap(templateId)),
        createArguments = Some(arguments)))

//  def exercise(templateId: lar.TemplateId, contractId: lar.ContractId, arguments: lav1.value.Record)
//    : CommandService.Error \/ lav1.commands.Command.Command.Exercise = {
//    lav1.commands.Command.Command.Exercise(
//      lav1.commands.ExerciseCommand(
//  }

  def submitAndWaitRequest(
      ledgerId: lar.LedgerId,
      applicationId: lar.ApplicationId,
      workflowId: lar.WorkflowId,
      commandId: lar.CommandId,
      ledgerEffectiveTime: Instant,
      maximumRecordTime: Instant,
      party: lar.Party,
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {

    val commands = lav1.commands.Commands(
      ledgerId = lar.LedgerId.unwrap(ledgerId),
      workflowId = lar.WorkflowId.unwrap(workflowId),
      applicationId = lar.ApplicationId.unwrap(applicationId),
      commandId = lar.CommandId.unwrap(commandId),
      party = lar.Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(ledgerEffectiveTime)),
      maximumRecordTime = Some(fromInstant(maximumRecordTime)),
      commands = Seq(lav1.commands.Command(command))
    )

    lav1.command_service.SubmitAndWaitRequest(Some(commands))
  }
}
