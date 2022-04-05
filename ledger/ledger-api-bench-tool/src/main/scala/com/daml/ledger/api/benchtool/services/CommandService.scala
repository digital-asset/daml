// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.services

import com.daml.ledger.api.benchtool.AuthorizationHelper
import com.daml.ledger.api.v1.command_service._
import com.daml.ledger.api.v1.commands.{Commands}
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.validation.NoLoggingValueValidator
import com.daml.lf.data.Ref.ChoiceName
import com.daml.lf.engine.script.Converter
import com.daml.lf.engine.script.ledgerinteraction.ScriptLedgerClient
import com.daml.lf.value.Value.ContractId
import io.grpc.Channel
import org.slf4j.LoggerFactory
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class CommandService(channel: Channel, authorizationToken: Option[String]) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val service: CommandServiceGrpc.CommandServiceStub =
    AuthorizationHelper.maybeAuthedService(authorizationToken)(CommandServiceGrpc.stub(channel))

  def submitAndWaitForTransactionTree(
      commands: Commands
  )(implicit ec: ExecutionContext): Future[Seq[ScriptLedgerClient.CommandResult]] =
    service
      .submitAndWaitForTransactionTree(new SubmitAndWaitRequest(Some(commands)))
      .recoverWith { case NonFatal(ex) =>
        Future.failed {
          logger.error(s"Command submission error. Details: ${ex.getLocalizedMessage}", ex)
          ex
        }
      }
      .map { transactionTree: SubmitAndWaitForTransactionTreeResponse =>
        val rootEvents: List[TreeEvent] = transactionTree.getTransaction.rootEventIds
          .map(evId => transactionTree.getTransaction.eventsById(evId))
          .toList
        val result: Either[String, Seq[ScriptLedgerClient.CommandResult]] =
          rootEvents.traverse(fromTreeEvent(_))
        result match {
          case Left(msg) => sys.error(msg)
          case Right(v) => v
        }
      }

  private def fromTreeEvent(ev: TreeEvent): Either[String, ScriptLedgerClient.CommandResult] =
    // NOTE: Copied from com.daml.lf.engine.script.ledgerinteraction.GrpcLedgerClient.submit
    ev match {
      case TreeEvent(TreeEvent.Kind.Created(created)) =>
        for {
          cid <- ContractId.fromString(created.contractId)
        } yield ScriptLedgerClient.CreateResult(cid)
      case TreeEvent(TreeEvent.Kind.Exercised(exercised)) =>
        for {
          result <- NoLoggingValueValidator
            .validateValue(exercised.getExerciseResult)
            .left
            .map(_.toString)
          templateId <- Converter.fromApiIdentifier(exercised.getTemplateId)
          choice <- ChoiceName.fromString(exercised.choice)
        } yield ScriptLedgerClient.ExerciseResult(templateId, choice, result)
      case TreeEvent(TreeEvent.Kind.Empty) =>
        sys.error("Invalid tree event Empty")
    }

}
