// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import java.time.Instant

import com.digitalasset.api.util.TimestampConversion.fromInstant
import com.digitalasset.http.CommandService.Error
import com.digitalasset.http.domain.Contract
import com.digitalasset.ledger.api.refinements.{ApiTypes => lar}
import com.digitalasset.ledger.api.{v1 => lav1}
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/
import scalaz.syntax.show._

object Commands extends StrictLogging {
  def create(
      templateId: lar.TemplateId,
      arguments: lav1.value.Record): lav1.commands.Command.Command.Create = {

    lav1.commands.Command.Command.Create(
      lav1.commands.CreateCommand(
        templateId = Some(lar.TemplateId.unwrap(templateId)),
        createArguments = Some(arguments)))
  }

  def exercise(
      templateId: lar.TemplateId,
      contractId: lar.ContractId,
      choice: lar.Choice,
      argument: lav1.value.Value): lav1.commands.Command.Command.Exercise = {

    lav1.commands.Command.Command.Exercise(
      lav1.commands.ExerciseCommand(
        templateId = Some(lar.TemplateId.unwrap(templateId)),
        contractId = lar.ContractId.unwrap(contractId),
        choice = lar.Choice.unwrap(choice),
        choiceArgument = Some(argument)
      )
    )
  }

  def submitAndWaitRequest(
      ledgerId: lar.LedgerId,
      applicationId: lar.ApplicationId,
      commandId: lar.CommandId,
      ledgerEffectiveTime: Instant,
      maximumRecordTime: Instant,
      party: lar.Party,
      command: lav1.commands.Command.Command): lav1.command_service.SubmitAndWaitRequest = {

    val commands = lav1.commands.Commands(
      ledgerId = lar.LedgerId.unwrap(ledgerId),
      applicationId = lar.ApplicationId.unwrap(applicationId),
      commandId = lar.CommandId.unwrap(commandId),
      party = lar.Party.unwrap(party),
      ledgerEffectiveTime = Some(fromInstant(ledgerEffectiveTime)),
      maximumRecordTime = Some(fromInstant(maximumRecordTime)),
      commands = Seq(lav1.commands.Command(command))
    )

    lav1.command_service.SubmitAndWaitRequest(Some(commands))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def contracts(tx: lav1.transaction.Transaction): Error \/ List[Contract[lav1.value.Value]] =
    Contract.fromTransaction(tx).leftMap(e => Error('contracts, e.shows))
}
