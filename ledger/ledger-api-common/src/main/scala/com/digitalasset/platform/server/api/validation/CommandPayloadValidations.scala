// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.ledger.api.v1.commands.{Command, Commands}

import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

trait CommandPayloadValidations extends CommandValidations with ErrorFactories {

  protected def ledgerId: String

  protected def validateCommands(cs: Commands): Either[Throwable, Unit] = {
    for {
      _ <- matchLedgerId(ledgerId)(cs.ledgerId)
      _ <- requireNonEmptyString(cs.commandId, "command_id")
      _ <- requireNonEmptyString(cs.party, "party")
      _ <- requireNonEmptyString(cs.workflowId, "workflow_id")
      _ <- requirePresence(cs.ledgerEffectiveTime, "ledger_effective_time")
      _ <- requirePresence(cs.maximumRecordTime, "maximum_record_time")
      _ <- cs.commands.toList.traverseU(validateCommand)
    } yield ()
  }
  protected def validateCommand(command: Command): Either[Throwable, Unit] = command.command match {
    case Command.Command.Create(create) =>
      for {
        _ <- requirePresence(create.createArguments, "create_arguments")
        _ <- requirePresence(create.templateId, "template_id")
      } yield ()
    case Command.Command.Exercise(ex) =>
      for {
        _ <- requirePresence(ex.choiceArgument, "choice_argument")
        _ <- requirePresence(ex.templateId, "template_id")
      } yield ()

    case Command.Command.CreateAndExercise(ce) =>
      for {
        _ <- requirePresence(ce.templateId, "template_id")
        _ <- requirePresence(ce.createArguments, "create_arguments")
        _ <- requireNonEmptyString(ce.choice, "choice")
        _ <- requirePresence(ce.choiceArgument, "choice_argument")
      } yield ()
    case _ => Left(missingField("command"))
  }
}
