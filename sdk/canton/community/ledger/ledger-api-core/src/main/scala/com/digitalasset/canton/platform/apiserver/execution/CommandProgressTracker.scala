// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.execution

import com.daml.error.utils.DecodedCantonError
import com.daml.ledger.api.v2.admin.command_inspection_service.{
  CommandState,
  CommandStatus as ApiCommandStatus,
  CommandUpdates,
  RequestStatistics,
}
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.completion.Completion
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Failure
import scala.util.control.NonFatal

final case class CommandStatus(
    started: CantonTimestamp,
    completed: Option[CantonTimestamp],
    completion: Completion,
    state: CommandState,
    commands: Seq[Command],
    requestStatistics: RequestStatistics,
    updates: CommandUpdates,
) extends PrettyPrinting {
  def toProto: ApiCommandStatus = {
    ApiCommandStatus(
      started = Some(started.toProtoTimestamp),
      completed = completed.map(_.toProtoTimestamp),
      completion = Some(completion),
      state = state,
      commands = commands,
      requestStatistics = Some(requestStatistics),
      updates = Some(updates),
    )
  }

  def decodedError: Option[DecodedCantonError] =
    completion.status.flatMap(s => DecodedCantonError.fromGrpcStatus(s).toOption)

  private implicit val prettyRequestStats: Pretty[RequestStatistics] = prettyOfClass(
    param("requestSize", _.requestSize),
    param("recipients", _.recipients),
    param("envelopes", _.envelopes),
  )

  private implicit val prettyUpdateStats: Pretty[CommandUpdates] = prettyOfClass(
    param("created", _.created.length),
    param("archived", _.archived.length),
    param("exercised", _.exercised),
    param("fetched", _.fetched),
    param("lookedUpByKey", _.lookedUpByKey),
  )

  private def nonEmptyUpdate(update: CommandUpdates): Boolean = {
    update.created.nonEmpty || update.archived.nonEmpty || update.exercised > 0 || update.fetched > 0 || update.lookedUpByKey > 0
  }

  override lazy val pretty: Pretty[CommandStatus] = prettyOfClass(
    param("commandId", _.completion.commandId.singleQuoted),
    param("started", _.started),
    paramIfDefined("completed", _.completed),
    param("state", _.state.toString().singleQuoted),
    param("completion", _.completion.status),
    paramIfDefined(
      "transactionId",
      x => Option.when(x.completion.updateId.nonEmpty)(x.completion.updateId.singleQuoted),
    ),
    paramIfDefined(
      "request",
      x => Option.when(x.requestStatistics.requestSize > 0)(x.requestStatistics),
    ),
    paramIfDefined(
      "update",
      x => Option.when(nonEmptyUpdate(x.updates))(x.updates),
    ),
  )

}

object CommandStatus {
  def fromProto(
      proto: ApiCommandStatus
  ): Either[ProtoDeserializationError, CommandStatus] = {
    val ApiCommandStatus(
      startedP,
      completedP,
      completionP,
      stateP,
      commandsP,
      requestStatisticsP,
      updatesP,
    ) = proto
    for {
      started <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoTimestamp,
        "started",
        startedP,
      )
      completed <- completedP
        .map(CantonTimestamp.fromProtoTimestamp(_).map(Some(_)))
        .getOrElse(Right(None))
      completion <- ProtoConverter.required("completion", completionP)
      requestsStatistics <- ProtoConverter.required("requestStatistics", requestStatisticsP)
      updates <- ProtoConverter.required("updates", updatesP)
    } yield CommandStatus(
      started = started,
      completed = completed,
      completion = completion,
      state = stateP,
      commands = commandsP,
      requestStatistics = requestsStatistics,
      updates = updates,
    )
  }
}

/** Result handle that allows to update a command with a respective result */
trait CommandResultHandle {

  def failedSync(err: StatusRuntimeException): Unit
  def internalErrorSync(err: Throwable): Unit

  def extractFailure[T](f: Future[T])(implicit executionContext: ExecutionContext): Future[T] = {
    f.transform {
      case ff @ Failure(err: StatusRuntimeException) =>
        failedSync(err)
        ff
      case ff @ Failure(NonFatal(err)) =>
        internalErrorSync(err)
        ff
      case rr => rr
    }
  }

  def recordEnvelopeSizes(batchSize: Int, numRecipients: Int, numEnvelopes: Int): Unit

  def recordTransactionImpact(
      transaction: com.digitalasset.daml.lf.transaction.SubmittedTransaction
  ): Unit

}

object CommandResultHandle {
  lazy val NoOp: CommandResultHandle = new CommandResultHandle {
    override def failedSync(err: StatusRuntimeException): Unit = ()
    override def internalErrorSync(err: Throwable): Unit = ()
    override def recordEnvelopeSizes(batchSize: Int, numRecipients: Int, numEnvelopes: Int): Unit =
      ()
    override def recordTransactionImpact(
        transaction: com.digitalasset.daml.lf.transaction.SubmittedTransaction
    ): Unit = ()
  }
}

/** Command progress tracker for debugging
  *
  * In order to track the progress of a command, we internally update the progress of the command
  * using this tracker trait, and expose the information on the API.
  *
  * This is in total violation of the CQRS pattern, but it is a necessary evil for debugging.
  */
trait CommandProgressTracker {

  def findCommandStatus(
      commandIdPrefix: String,
      state: CommandState,
      limit: Int,
  ): Future[Seq[CommandStatus]]

  def registerCommand(
      commandId: String,
      submissionId: Option[String],
      applicationId: String,
      commands: Seq[Command],
      actAs: Set[String],
  )(implicit traceContext: TraceContext): CommandResultHandle

  def findHandle(
      commandId: String,
      applicationId: String,
      actAs: Seq[String],
      submissionId: Option[String],
  ): CommandResultHandle

  def processLedgerUpdate(update: Traced[TransactionLogUpdate]): Unit

}

object CommandProgressTracker {
  lazy val NoOp: CommandProgressTracker = new CommandProgressTracker {
    override def findCommandStatus(
        commandId: String,
        state: CommandState,
        limit: Int,
    ): Future[Seq[CommandStatus]] = Future.successful(Seq.empty)

    override def registerCommand(
        commandId: String,
        submissionId: Option[String],
        applicationId: String,
        commands: Seq[Command],
        actAs: Set[String],
    )(implicit traceContext: TraceContext): CommandResultHandle = CommandResultHandle.NoOp

    override def findHandle(
        commandId: String,
        applicationId: String,
        actAs: Seq[String],
        submissionId: Option[String],
    ): CommandResultHandle =
      CommandResultHandle.NoOp

    override def processLedgerUpdate(update: Traced[TransactionLogUpdate]): Unit = ()
  }
}
