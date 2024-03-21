// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import ammonite.Main
import ammonite.interp.Interpreter
import ammonite.runtime.Frame
import ammonite.util.Res.{Exception, Failing, Failure, Success}
import ammonite.util.*
import cats.syntax.either.*
import com.digitalasset.canton.console.HeadlessConsole.{
  HeadlessConsoleError,
  convertAmmoniteResult,
  createInterpreter,
  initializePredef,
  runCode,
}
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.NoTracing
import com.digitalasset.canton.util.ErrorUtil
import os.PathConvertible.*

import java.io.File
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.util.Try

class HeadlessConsole(
    consoleEnvironment: ConsoleEnvironment,
    transformer: Main => Main = identity,
    logger: TracedLogger,
) extends AutoCloseable {

  val (lock, baseOptions) =
    AmmoniteConsoleConfig.create(
      consoleEnvironment.environment.config.parameters.console,
      predefCode = "",
      welcomeBanner = None,
      isRepl = false,
      logger,
    )

  private val interpreterO = new AtomicReference[Option[Interpreter]](None)
  private val currentLine = new AtomicInteger(10000000)

  def init(): Either[HeadlessConsoleError, Unit] = {
    val options = transformer(baseOptions)
    for {
      interpreter <- Try(createInterpreter(options)).toEither.leftMap(
        HeadlessConsole.RuntimeError("Failed to initialize console", _)
      )
      bindings <- consoleEnvironment.bindings.leftMap(
        HeadlessConsole.RuntimeError("Unable to create the console bindings", _)
      )
      _ <- initializePredef(
        interpreter,
        bindings,
        consoleEnvironment.predefCode(_),
        logger,
      )
    } yield {
      interpreterO.set(Some(interpreter))
    }
  }

  private def runModule(
      code: String,
      path: Option[File] = None,
  ): Either[HeadlessConsoleError, Unit] =
    for {
      interpreter <- interpreterO
        .get()
        .toRight(HeadlessConsole.CompileError("Interpreter is not initialized"))
      _ <- runCode(interpreter, code, path, logger)
    } yield ()

  def runLine(line: String): Either[HeadlessConsoleError, Unit] = for {
    interpreter <- interpreterO
      .get()
      .toRight(HeadlessConsole.CompileError("Interpreter is not initialized"))
    _ <- convertAmmoniteResult(
      interpreter
        .processExec(line, currentLine.incrementAndGet(), () => ()),
      logger,
    )
  } yield ()

  override def close(): Unit = {
    lock.release()
  }
}

/** Creates an interpreter but with matching bindings to the InteractiveConsole for running scripts non-interactively
  */
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object HeadlessConsole extends NoTracing {

  sealed trait HeadlessConsoleError

  final case class CompileError(message: String) extends HeadlessConsoleError {
    override def toString: String = message
  }

  final case class IoError(message: String) extends HeadlessConsoleError {
    override def toString: String = message
  }

  final case class RuntimeError(message: String, cause: Throwable) extends HeadlessConsoleError {
    override def toString: String = {
      val messageWithSeparator = if (message.isEmpty) "" else message + " "
      val exceptionInfo = ErrorUtil.messageWithStacktrace(cause)
      messageWithSeparator + exceptionInfo
    }
  }

  def run(
      consoleEnvironment: ConsoleEnvironment,
      code: String,
      path: Option[File] = None,
      transformer: Main => Main = identity,
      logger: TracedLogger,
  ): Either[HeadlessConsoleError, Unit] = {
    val console = new HeadlessConsole(consoleEnvironment, transformer, logger)
    try {
      for {
        _ <- console.init()
        _ <- console.runModule(code, path)
      } yield ()
    } finally {
      console.close()
    }
  }

  private def initializePredef(
      interpreter: Interpreter,
      bindings: IndexedSeq[Bind[_]],
      interactivePredef: Boolean => String,
      logger: TracedLogger,
  ): Either[HeadlessConsoleError, Unit] = {
    val bindingsPredef = generateBindPredef(bindings)

    val holder = Seq(
      (
        // This has to match the object name of the implementation that extends APIHolder[_}
        objectClassNameWithoutSuffix(BindingsBridge.getClass),
        "canton",
        BindingsHolder(bindings),
      )
    )

    val result = interpreter.initializePredef(
      basePredefs = Seq(
        PredefInfo(Name("BindingsPredef"), bindingsPredef, hardcoded = false, None),
        PredefInfo(
          Name("CantonImplicitPredef"),
          interactivePredef(false),
          hardcoded = false,
          None,
        ),
      ),
      customPredefs = Seq(),
      extraBridges = holder,
    )

    // convert to an either and then map error if set to our own types
    result.toLeft(()).left.map(err => convertAmmoniteError(err._1, logger))
  }

  private def runCode(
      interpreter: Interpreter,
      code: String,
      path: Option[File],
      logger: TracedLogger,
  ): Either[HeadlessConsoleError, Unit] = {
    // the source details for our wrapper object that our code is compiled into
    val source = Util.CodeSource(
      wrapperName = Name("canton-script"),
      flexiblePkgName = Seq(Name("interpreter")),
      pkgRoot = Seq(Name("ammonite"), Name("canton")), // has to be rooted under ammonite
      path.map(path => os.Path(path.getAbsolutePath)),
    )

    val result = interpreter.processModule(code, source, autoImport = false, "", hardcoded = false)

    convertAmmoniteResult(result, logger)
  }

  /** Converts a return value from Ammonite into:
    *  - Unit if successful
    *  - Our own error hierarchy if the Ammonite error could be mapped
    * @throws java.lang.RuntimeException If the value is unknown
    */
  private def convertAmmoniteResult(
      result: Res[_],
      logger: TracedLogger,
  ): Either[HeadlessConsoleError, Unit] =
    result match {
      case Success(_) =>
        Right(())
      case failing: Failing => Left(convertAmmoniteError(failing, logger))
      case unexpected =>
        logger.error("Unexpected result from ammonite: {}", unexpected)
        sys.error("Unexpected result from ammonite")
    }

  /** Converts a failing return value from Ammonite into our own error types.
    * @throws java.lang.RuntimeException If the failing error is unknown
    */
  private def convertAmmoniteError(result: Failing, logger: TracedLogger): HeadlessConsoleError =
    result match {
      case Failure(msg) => CompileError(msg)
      case Exception(cause, msg) => RuntimeError(msg, cause)
      case unexpected =>
        logger.error("Unexpected error result from ammonite: {}", unexpected)
        sys.error("Unexpected error result from ammonite")
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private def createInterpreter(options: Main): Interpreter = {
    val (colorsRef, printer) = Interpreter.initPrinters(
      options.colors,
      options.outputStream,
      options.errorStream,
      options.verboseOutput,
    )
    val frame = Frame.createInitial()

    new Interpreter(
      compilerBuilder = ammonite.compiler.CompilerBuilder,
      parser = ammonite.compiler.Parsers,
      printer = printer,
      storage = options.storageBackend,
      wd = options.wd,
      colors = colorsRef,
      verboseOutput = options.verboseOutput,
      getFrame = () => frame,
      createFrame = () => sys.error("Session loading / saving is not supported"),
      initialClassLoader = null,
      replCodeWrapper = options.replCodeWrapper,
      scriptCodeWrapper = options.scriptCodeWrapper,
      alreadyLoadedDependencies = options.alreadyLoadedDependencies,
    )
  }

  private def generateBindPredef(binds: IndexedSeq[Bind[_]]): String =
    binds.zipWithIndex
      .map { case (b, idx) =>
        s"""
             |val ${b.name} = com.digitalasset.canton.console
             | .BindingsBridge
             | .value0
             | .bindings($idx)
             | .value
             | .asInstanceOf[${b.typeTag.tpe}]
      """.stripMargin
      }
      .mkString(System.lineSeparator)

}
