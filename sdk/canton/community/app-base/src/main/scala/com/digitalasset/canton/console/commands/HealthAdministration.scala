// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import better.files.File
import ch.qos.logback.classic.Level
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusElement
import com.digitalasset.canton.admin.api.client.commands.{
  GrpcAdminCommand,
  StatusAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  NodeStatus,
  WaitingForId,
  WaitingForInitialization,
  WaitingForNodeTopology,
}
import com.digitalasset.canton.admin.health.v30
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.CommandErrors.CommandError
import com.digitalasset.canton.console.ConsoleMacros.utils
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  CantonHealthAdministration,
  CommandSuccessful,
  ConsoleCommandResult,
  ConsoleEnvironment,
  Help,
  Helpful,
}
import com.digitalasset.canton.grpc.FileStreamObserver
import io.grpc.Context

import java.util.concurrent.atomic.AtomicReference

abstract class HealthAdministration[S <: NodeStatus.Status](
    runner: AdminCommandRunner,
    consoleEnvironment: ConsoleEnvironment,
) extends Helpful {
  private val initializedCache = new AtomicReference[Boolean](false)
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts

  import runner.*

  protected def nodeStatusCommand: GrpcAdminCommand[?, ?, NodeStatus[S]]

  @Help.Summary("Get human (and machine) readable status information")
  def status: NodeStatus[S] =
    consoleEnvironment.run {
      CommandSuccessful(adminCommand(nodeStatusCommand) match {
        case CommandSuccessful(success) => success
        case err: CommandError => NodeStatus.Failure(err.cause)
      })
    }

  @Help.Summary("Returns true if the node has an identity")
  def has_identity(): Boolean = adminCommand(
    TopologyAdminCommands.Init.GetId()
  ).toEither.isRight

  @Help.Summary("Wait for the node to have an identity")
  def wait_for_identity(): Unit = waitFor(has_identity())

  @Help.Summary(
    "Creates a zip file containing diagnostic information about the canton process running this node"
  )
  def dump(
      outputFile: File = CantonHealthAdministration.defaultHealthDumpName,
      timeout: NonNegativeDuration = timeouts.unbounded,
      chunkSize: Option[Int] = None,
  ): String = consoleEnvironment.run {
    val responseObserver =
      new FileStreamObserver[v30.HealthDumpResponse](outputFile, _.chunk)

    def call: ConsoleCommandResult[Context.CancellableContext] =
      adminCommand(new StatusAdminCommands.GetHealthDump(responseObserver, chunkSize))

    processResult(
      call,
      responseObserver.result,
      timeout,
      "Generating health dump",
      cleanupOnError = () => outputFile.delete(),
    ).map(_ => outputFile.pathAsString)
  }

  private def runningCommand =
    adminCommand(NodeStatusElement(nodeStatusCommand, _.isRunning))

  private def initializedCommand =
    adminCommand(NodeStatusElement(nodeStatusCommand, _.isInitialized))

  private def falseIfUnreachable(command: ConsoleCommandResult[Boolean]): Boolean =
    consoleEnvironment.run(CommandSuccessful(command match {
      case CommandSuccessful(result) => result
      case _: CommandError => false
    }))

  @Help.Summary("Check if the node is running")
  def is_running(): Boolean =
    // in case the node is not reachable, we assume it is not running
    falseIfUnreachable(runningCommand)

  @Help.Summary("Check if the node is ready for setting the node's id")
  def is_ready_for_id(): Boolean =
    falseIfUnreachable(
      adminCommand(
        NodeStatusElement(
          nodeStatusCommand,
          NodeStatusElement.isWaitingForExternalInput(_, WaitingForId),
        )
      )
    )

  @Help.Summary("Check if the node is ready for uploading the node's identity topology")
  def is_ready_for_node_topology(): Boolean = falseIfUnreachable(
    adminCommand(
      NodeStatusElement(
        nodeStatusCommand,
        NodeStatusElement.isWaitingForExternalInput(_, WaitingForNodeTopology),
      )
    )
  )

  @Help.Summary("Check if the node is ready for initialization")
  def is_ready_for_initialization(): Boolean =
    falseIfUnreachable(
      adminCommand(
        NodeStatusElement(
          nodeStatusCommand,
          NodeStatusElement.isWaitingForExternalInput(_, WaitingForInitialization),
        )
      )
    )

  @Help.Summary("Check if the node is running and is the active instance (mediator, participant)")
  def active: Boolean = status match {
    case NodeStatus.Success(status) => status.active
    case NodeStatus.NotInitialized(active, _) => active
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
  def wait_for_running(): Unit = waitFor(is_running())

  @Help.Summary("Wait for the node to be initialized")
  def wait_for_initialized(): Unit =
    waitFor(initializedCache.updateAndGet {
      case false =>
        // in case the node is not reachable, we return false instead of throwing an error in order to keep retrying
        falseIfUnreachable(initializedCommand)
      case x => x
    })

  @Help.Summary("Wait for the node to be ready for setting the node's id")
  def wait_for_ready_for_id(): Unit = waitFor(is_ready_for_id())
  @Help.Summary("Wait for the node to be ready for uploading the node's identity topology")
  def wait_for_ready_for_node_topology(): Unit = waitFor(is_ready_for_node_topology())
  @Help.Summary("Wait for the node to be ready for initialization")
  def wait_for_ready_for_initialization(): Unit = waitFor(is_ready_for_initialization())

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
