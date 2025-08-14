// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{CantonConfig, NonNegativeDuration}
import com.digitalasset.canton.console.CommandErrors.ConsoleTimeout
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.environment.{CantonNode, CantonNodeBootstrap}
import com.digitalasset.canton.logging.{NamedLogging, TracedLogger}
import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec

/** Support for running an admin command
  */
trait AdminCommandRunner {

  /** Run a GRPC admin command and return its result. Most of the commands are only defined for the
    * GRPC interface, so we default to showing an error message if the command is called for a node
    * configured with an HTTP interface.
    */
  protected[console] def adminCommand[Result](
      grpcCommand: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result]

  protected[console] def tracedLogger: TracedLogger

}

object AdminCommandRunner {
  def retryUntilTrue(timeout: NonNegativeDuration)(
      condition: => Boolean
  ): ConsoleCommandResult[Unit] = {
    val deadline = timeout.asFiniteApproximation.fromNow
    @tailrec
    def go(): ConsoleCommandResult[Unit] = {
      val res = condition
      if (!res) {
        if (deadline.hasTimeLeft()) {
          Threading.sleep(100)
          go()
        } else {
          ConsoleTimeout.Error(timeout.asJavaApproximation)
        }
      } else {
        CommandSuccessful(())
      }
    }
    go()
  }
}

/** Support for running ledgerApi commands
  */
trait LedgerApiCommandRunner {

  protected[console] def ledgerApiCommand[Result](
      command: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result]

  protected[console] def token: Option[String]
}

trait PublicApiCommandRunner {
  protected[console] def publicApiCommand[Result](
      command: GrpcAdminCommand[_, _, Result]
  ): ConsoleCommandResult[Result]
}

/** Support for inspecting the instance */
trait BaseInspection[+I <: CantonNode] {

  def underlying: Option[I] =
    runningNode.flatMap(_.getNode)

  protected[console] def runningNode: Option[CantonNodeBootstrap[I]]
  protected[console] def startingNode: Option[CantonNodeBootstrap[I]]
  protected[console] def name: String

  protected[console] def access[T](ops: I => T): T =
    ops(
      runningNode
        .getOrElse(throw new IllegalArgumentException(s"instance $name is not running"))
        .getNode
        .getOrElse(
          throw new IllegalArgumentException(
            s"instance $name is still starting or awaiting manual initialisation."
          )
        )
    )

  protected[canton] def crypto: Crypto =
    runningNode
      .flatMap(_.crypto)
      .getOrElse(throw new IllegalArgumentException(s"instance $name is not running."))

}

trait FeatureFlagFilter extends NamedLogging {

  protected def consoleEnvironment: ConsoleEnvironment

  protected def cantonConfig: CantonConfig = consoleEnvironment.environment.config

  protected def check[T](flag: FeatureFlag)(command: => T): T =
    FeatureFlagFilter.checkEnabled(noTracingLogger)(
      consoleEnvironment.featureSet.contains(flag),
      flag.configName,
      command,
    )
}

object FeatureFlagFilter {
  private def checkEnabled[T](logger: Logger)(flag: Boolean, config: String, command: => T): T =
    if (flag) {
      command
    } else {
      val hintO = Thread
        .currentThread()
        .getStackTrace
        // java.base/java.lang.Thread.getStackTrace
        .drop(1)
        // method in this class
        .dropWhile { element =>
          element.toString.contains(
            "com.digitalasset.canton.console.FeatureFlagFilter"
          ) || element.toString.contains(".check(")
        }
        .headOption

      val hint = hintO.fold("")(hint => s"\nCall site: $hint")

      logger.error(s"""The command is currently disabled.
          |You need to enable it explicitly by setting `canton.features.$config = yes` in your Canton configuration file (`.conf`).$hint
          |""".stripMargin)

      throw new CommandFailure()
    }

  private[canton] def check[T](logger: Logger, consoleEnvironment: ConsoleEnvironment)(
      flag: FeatureFlag
  )(command: => T): T =
    checkEnabled(logger)(consoleEnvironment.featureSet.contains(flag), flag.configName, command)
}
