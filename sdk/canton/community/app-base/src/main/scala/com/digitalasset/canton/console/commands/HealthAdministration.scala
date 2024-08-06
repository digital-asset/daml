// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import ch.qos.logback.classic.Level
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.admin.api.client.commands.{
  StatusAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.NodeStatus
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.CommandErrors.{CommandError, GenericCommandError}
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CantonHealthAdministration,
  CommandErrors,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  Help,
  Helpful,
}
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.health.admin.v0.HealthDumpChunk
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.ResourceUtil
import io.grpc.StatusRuntimeException

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Await, Promise, TimeoutException}

abstract class HealthAdministrationCommon[S <: NodeStatus.Status](
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    deserialize: v0.NodeStatus.Status => ParsingResult[S],
) extends Helpful {
  private val initializedCache = new AtomicReference[Boolean](false)
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  import runner.*

  @Help.Summary("Get human (and machine) readable status info")
  def status: NodeStatus[S] = consoleEnvironment.run {
    CommandSuccessful(adminCommand(new StatusAdminCommands.GetStatus[S](deserialize)) match {
      case CommandSuccessful(success) => success
      case err: CommandError => NodeStatus.Failure(err.cause)
    })
  }

  @Help.Summary("Returns true if the node has an identity")
  def has_identity(): Boolean

  @Help.Summary("Wait for the node to have an identity")
  @Help.Description(
    """This is specifically useful for the Domain Manager which needs its identity to be ready for bootstrapping,
      | but for which we can't rely on wait_for_initialized() because it will be initialized only after being bootstrapped."""
  )
  def wait_for_identity(): Unit = waitFor(has_identity())

  @Help.Summary(
    "Creates a zip file containing diagnostic information about the canton process running this node"
  )
  def dump(
      outputFile: File = CantonHealthAdministration.defaultHealthDumpName,
      timeout: NonNegativeDuration = timeouts.unbounded,
      chunkSize: Option[Int] = None,
  ): String = consoleEnvironment.run {
    val requestComplete = Promise[String]()
    val responseObserver =
      new GrpcByteChunksToFileObserver[HealthDumpChunk](outputFile, requestComplete)

    def call = consoleEnvironment.run {
      adminCommand(new StatusAdminCommands.GetHealthDump(responseObserver, chunkSize))
    }

    try {
      ResourceUtil.withResource(call) { _ =>
        CommandSuccessful(
          Await.result(requestComplete.future, timeout.duration)
        )
      }
    } catch {
      case sre: StatusRuntimeException =>
        GenericCommandError(GrpcError("Generating health dump file", "dump", sre).toString)
      case _: TimeoutException =>
        outputFile.delete(swallowIOExceptions = true)
        CommandErrors.ConsoleTimeout.Error(timeout.asJavaApproximation)
    }
  }

  private def runningCommand =
    adminCommand(
      StatusAdminCommands.IsRunning
    )
  private def initializedCommand =
    adminCommand(
      StatusAdminCommands.IsInitialized
    )

  def falseIfUnreachable(command: ConsoleCommandResult[Boolean]): Boolean =
    consoleEnvironment.run(CommandSuccessful(command match {
      case CommandSuccessful(result) => result
      case _: CommandError => false
    }))

  @Help.Summary("Check if the node is running")
  def running(): Boolean =
    // in case the node is not reachable, we assume it is not running
    falseIfUnreachable(runningCommand)

  @Help.Summary("Check if the node is running and is the active instance (mediator, participant)")
  def active: Boolean = status match {
    case NodeStatus.Success(status) => status.active
    case NodeStatus.NotInitialized(active) => active
    case _ => false
  }

  @Help.Summary("Returns true if node has been initialized.")
  def initialized(): Boolean = initializedCache.updateAndGet {
    case false =>
      // in case the node is not reachable, we cannot assume it is not initialized, because it could have been initialized in the past
      // and it's simply not running at the moment. so we'll allow the command to throw an error here
      consoleEnvironment.run(initializedCommand)
    case x => x
  }

  @Help.Summary("Wait for the node to be running")
  def wait_for_running(): Unit = waitFor(running())

  @Help.Summary("Wait for the node to be initialized")
  def wait_for_initialized(): Unit =
    waitFor(initializedCache.updateAndGet {
      case false =>
        // in case the node is not reachable, we return false instead of throwing an error in order to keep retrying
        falseIfUnreachable(initializedCommand)
      case x => x
    })

  protected def waitFor(condition: => Boolean): Unit =
    // all calls here are potentially unbounded. we do not know how long it takes
    // for a node to start or for a node to become initialised. so we use the unbounded
    // timeout
    utils.retry_until_true(timeout = consoleEnvironment.commandTimeouts.unbounded)(condition)

  @Help.Summary("Change the log level of the process")
  @Help.Description(
    "If the default logback configuration is used, this will change the log level of the process."
  )
  def set_log_level(level: Level): Unit = consoleEnvironment.run {
    adminCommand(
      new StatusAdminCommands.SetLogLevel(level)
    )
  }

  @Help.Summary("Show the last errors logged")
  @Help.Description(
    """Returns a map with the trace-id as key and the most recent error messages as value. Requires that --log-last-errors is enabled (and not turned off)."""
  )
  def last_errors(): Map[String, String] = consoleEnvironment.run {
    adminCommand(
      new StatusAdminCommands.GetLastErrors()
    )
  }

  @Help.Summary("Show all messages logged with the given traceId in a recent interval")
  @Help.Description(
    "Returns a list of buffered log messages associated to a given trace-id. Usually, the trace-id is taken from last_errors()"
  )
  def last_error_trace(traceId: String): Seq[String] = consoleEnvironment.run {
    adminCommand(
      new StatusAdminCommands.GetLastErrorTrace(traceId)
    )
  }

}

abstract class HealthAdministration[S <: NodeStatus.Status](
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
    deserialize: v0.NodeStatus.Status => ParsingResult[S],
) extends HealthAdministrationCommon[S](runner, consoleEnvironment, deserialize) {

  override def has_identity(): Boolean = runner
    .adminCommand(
      TopologyAdminCommands.Init.GetId()
    )
    .toEither
    .isRight

  protected def nodeStatusCommand: NodeStatusCommand[S, _, _]

  @Help.Summary("Get human (and machine) readable participant status information")
  override def status: NodeStatus[S] = consoleEnvironment.run {
    val commandResult = runner.adminCommand(nodeStatusCommand)

    commandResult.toEither match {
      case Left(errorMessage) => CommandSuccessful(NodeStatus.Failure(errorMessage))
      /* For backward compatibility:
      Assumes getting a left of gRPC code means that the node specific status endpoint
      is not available (because that Canton version does not include it), and thus we
      want to fall back to the original status command.
       */
      case Right(Left(_code)) => CommandSuccessful(super.status)
      case Right(Right(nodeStatus)) => CommandSuccessful(nodeStatus)
    }
  }
}
