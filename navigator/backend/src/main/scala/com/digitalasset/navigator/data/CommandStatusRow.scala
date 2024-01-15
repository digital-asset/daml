// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.data

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.model._

import scala.util.{Failure, Success, Try}
import scalaz.syntax.tag._

final case class CommandStatusRow(
    commandId: String,
    isCompleted: Boolean,
    subclassType: String,
    code: Option[String],
    details: Option[String],
    transactionId: Option[String],
) {

  def toCommandStatus(
      transactionById: ApiTypes.TransactionId => Try[Option[Transaction]]
  ): Try[CommandStatus] = {
    subclassType match {
      case "CommandStatusWaiting" =>
        Success(CommandStatusWaiting())
      case "CommandStatusError" =>
        (for {
          c <- code
          d <- details
        } yield {
          CommandStatusError(c, d)
        }).fold[Try[CommandStatus]](
          Failure(
            DeserializationFailed(s"Failed to deserialize CommandStatusError from row: $this")
          )
        )(
          Success(_)
        )
      case "CommandStatusSuccess" =>
        transactionId.map { tId =>
          transactionById(ApiTypes.TransactionId(tId))
        } match {
          case Some(Success(Some(tx: Transaction))) => Success(CommandStatusSuccess(tx))
          case Some(Failure(e)) =>
            Failure(
              RecordNotFound(
                s"Failed to load transaction $transactionId for CommandStatus with commandId: $commandId. Exception: ${e.getMessage}"
              )
            )
          case Some(Success(None)) =>
            Failure(
              RecordNotFound(
                s"Failed to load transaction $transactionId for CommandStatus with commandId: $commandId"
              )
            )
          case None =>
            Failure(
              DeserializationFailed(s"TransactionId is missing for CommandStatusSuccess row: $this")
            )
        }
      case "CommandStatusUnknown" =>
        Success(CommandStatusUnknown())
      case s => Failure(DeserializationFailed(s"unknown subclass type for CommandStatus: $s"))
    }
  }
}

object CommandStatusRow {

  def fromCommandStatus(commandId: ApiTypes.CommandId, cs: CommandStatus): CommandStatusRow = {
    cs match {
      case w: CommandStatusWaiting =>
        CommandStatusRow(commandId.unwrap, w.isCompleted, "CommandStatusWaiting", None, None, None)
      case e: CommandStatusError =>
        CommandStatusRow(
          commandId.unwrap,
          e.isCompleted,
          "CommandStatusError",
          Some(e.code),
          Some(e.details),
          None,
        )
      case s: CommandStatusSuccess =>
        CommandStatusRow(
          commandId.unwrap,
          s.isCompleted,
          "CommandStatusSuccess",
          None,
          None,
          Some(s.tx.id.unwrap),
        )
      case u: CommandStatusUnknown =>
        CommandStatusRow(commandId.unwrap, u.isCompleted, "CommandStatusUnknown", None, None, None)
    }
  }
}
