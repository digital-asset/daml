// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.console.{HeadlessConsole, InteractiveConsole}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}

import java.io.{File, OutputStream, StringWriter}
import scala.io.Source
import scala.util.control.NonFatal

/** Result for exposing the process exit code.
  * All logging is expected to take place inside of the runner.
  */
trait Runner[E <: Environment] extends NamedLogging {

  def run(environment: E): Unit
}

class ServerRunner[E <: Environment](
    bootstrapScript: Option[CantonScript] = None,
    override val loggerFactory: NamedLoggerFactory,
) extends Runner[E]
    with NoTracing {

  def run(environment: E): Unit =
    try {
      def start(): Unit = {
        environment
          .startAll() match {
          case Right(_) => logger.info("Canton started")
          case Left(error) =>
            logger.error(s"Canton startup encountered problems: $error")
            // give up as we couldn't start everything successfully
            sys.exit(1)
        }
      }

      def startWithBootstrap(script: CantonScript): Unit =
        ConsoleScriptRunner.run(environment, script, logger = logger) match {
          case Right(_unit) => logger.info("Bootstrap script successfully executed.")
          case Left(err) =>
            logger.error(s"Bootstrap script terminated with an error: $err")
            sys.exit(3)
        }

      bootstrapScript.fold(start())(startWithBootstrap)
    } catch {
      case ex: Throwable =>
        logger.error(s"Unexpected error while running server: ${ex.getMessage}")
        logger.info("Exception causing error is:", ex)
        sys.exit(2)
    }
}

class ConsoleInteractiveRunner[E <: Environment](
    noTty: Boolean = false,
    bootstrapScript: Option[CantonScript],
    override val loggerFactory: NamedLoggerFactory,
) extends Runner[E] {
  def run(environment: E): Unit = {
    val success =
      try {
        val consoleEnvironment = environment.createConsole()
        InteractiveConsole(consoleEnvironment, noTty, bootstrapScript, logger)
      } catch {
        case NonFatal(_) => false
      }
    sys.exit(if (success) 0 else 1)
  }
}

class ConsoleScriptRunner[E <: Environment](
    scriptPath: CantonScript,
    override val loggerFactory: NamedLoggerFactory,
) extends Runner[E] {
  private val Ok = 0
  private val Error = 1

  override def run(environment: E): Unit = {
    val exitCode =
      ConsoleScriptRunner.run(environment, scriptPath, logger) match {
        case Right(_unit) =>
          Ok
        case Left(err) =>
          logger.error(s"Script execution failed: $err")(TraceContext.empty)
          Error
      }

    sys.exit(exitCode)
  }
}

private class CopyOutputWriter(parent: OutputStream, logger: TracedLogger)
    extends OutputStream
    with NoTracing {
  val buf = new StringWriter()
  override def write(b: Int): Unit = {
    if (b == '\n') {
      // strip the ansi color commands from the string
      val output = buf.toString.replaceAll("\u001B\\[[;\\d]*m", "")
      logger.info(s"Console stderr output: ${output}")
      buf.getBuffer.setLength(0)
    } else {
      buf.write(b)
    }
    parent.write(b)
  }
}

sealed trait CantonScript {
  def path: Option[File]
  def read(): Either[HeadlessConsole.IoError, String]
}
final case class CantonScriptFromFile(scriptPath: File) extends CantonScript {
  override val path = Some(scriptPath)
  override def read(): Either[HeadlessConsole.IoError, String] =
    readScript(scriptPath)

  private def readScript(scriptPath: File): Either[HeadlessConsole.IoError, String] =
    for {
      path <- verifyScriptCanBeRead(scriptPath)
      content <- readScriptContent(path)
    } yield content

  private def verifyScriptCanBeRead(scriptPath: File): Either[HeadlessConsole.IoError, File] =
    Either.cond(
      scriptPath.canRead,
      scriptPath,
      HeadlessConsole.IoError(s"Script file not readable: $scriptPath"),
    )

  private def readScriptContent(scriptPath: File): Either[HeadlessConsole.IoError, String] = {
    val source = Source.fromFile(scriptPath)
    try {
      Right(source.mkString)
    } catch {
      case NonFatal(ex: Throwable) =>
        Left(HeadlessConsole.IoError(s"Failed to read script file: $ex"))
    } finally {
      source.close()
    }
  }
}

object ConsoleScriptRunner extends NoTracing {
  def apply[E <: Environment](
      scriptPath: File,
      loggerFactory: NamedLoggerFactory,
  ): ConsoleScriptRunner[E] =
    new ConsoleScriptRunner[E](CantonScriptFromFile(scriptPath), loggerFactory)
  def run[E <: Environment](
      environment: E,
      scriptPath: File,
      logger: TracedLogger,
  ): Either[HeadlessConsole.HeadlessConsoleError, Unit] =
    run(environment, CantonScriptFromFile(scriptPath), logger)

  def run[E <: Environment](
      environment: E,
      cantonScript: CantonScript,
      logger: TracedLogger,
  ): Either[HeadlessConsole.HeadlessConsoleError, Unit] = {
    val consoleEnvironment = environment.createConsole()
    try {
      for {
        scriptCode <- cantonScript.read()
        _ <- HeadlessConsole.run(
          consoleEnvironment,
          scriptCode,
          cantonScript.path,
          // clone error stream such that we also log the error message
          // unfortunately, this means that if somebody outputs INFO to stdout,
          // he will observe the error twice
          transformer = x => x.copy(errorStream = new CopyOutputWriter(x.errorStream, logger)),
          logger = logger,
        )
      } yield ()
    } finally {
      consoleEnvironment.closeChannels()
    }
  }
}
