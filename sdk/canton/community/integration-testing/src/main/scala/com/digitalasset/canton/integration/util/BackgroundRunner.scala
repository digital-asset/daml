// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import better.files.File
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{NoTracing, TraceContext}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import org.slf4j.event.Level

import java.io.{IOException, InputStream, StringWriter}
import java.nio.BufferOverflowException
import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*

/** Handler that exposes lifecycle methods for managing a background process.
  * @tparam ProcessInfo
  *   type of information about the process that will show up in error messages
  */
class BackgroundRunnerHandler[ProcessInfo](
    timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with AutoCloseable
    with NoTracing {

  private sealed trait ProcessHandle {
    def info: ProcessInfo
  }
  private case class Configured(
      name: String,
      command: Seq[String],
      addEnvironment: Map[String, String],
      info: ProcessInfo,
  ) extends ProcessHandle {
    def start(): Running =
      Running(
        name,
        runner = new BackgroundRunner(name, command, addEnvironment, timeouts, loggerFactory),
        info,
      )
  }
  private case class Running(name: String, runner: BackgroundRunner, info: ProcessInfo)
      extends ProcessHandle {
    def kill(force: Boolean = false): Configured = {
      runner.kill(force)
      Configured(name, runner.command, runner.addEnvironment, info)
    }
    def restart(): Running =
      Running(name, runner.restart(), info)
  }

  private val external = new TrieMap[String, ProcessHandle]()

  def tryAdd(
      instanceName: String,
      command: Seq[String],
      addEnvironment: Map[String, String],
      info: ProcessInfo,
      manualStart: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    ErrorUtil.requireArgument(
      !external.contains(instanceName),
      s"key already exists $instanceName",
    )
    ErrorUtil.requireArgument(
      command.nonEmpty,
      s"you've supplied empty commands for $instanceName",
    )
    val configured = Configured(instanceName, command, addEnvironment, info)
    external.put(instanceName, if (!manualStart) configured.start() else configured).discard
  }

  /** Stop and remove a background process. Idempotent as it doesn't require that the background
    * process was previously added.
    */
  def stopAndRemove(instanceName: String): Unit = {
    val prev = external.remove(instanceName)
    prev match {
      case Some(processHandle: Running) => processHandle.kill().discard
      case _ => ()
    }
  }

  def tryIsRunning(instanceName: String): Boolean =
    external.get(instanceName) match {
      case Some(_: Configured) => false
      case Some(_: Running) => true
      case None =>
        ErrorUtil.internalError(new IllegalStateException(s"$instanceName is not registered"))
    }

  def tryStart(instanceName: String): Unit =
    perform(
      instanceName,
      {
        case a: Configured =>
          noTracingLogger.info(s"Starting external process for $instanceName")
          a.start()
        case Running(_, _, _) =>
          ErrorUtil.internalError(
            new IllegalStateException(
              s"can not start $instanceName as instance is already running"
            )
          )
      },
    )

  private def perform(instanceName: String, func: ProcessHandle => ProcessHandle): Unit =
    external.get(instanceName) match {
      case Some(item) =>
        external.update(instanceName, func(item))
      case None =>
        ErrorUtil.internalError(
          new IllegalArgumentException(
            s"no such instance $instanceName configured as remote running instance. have ${external.keys}"
          )
        )
    }

  def tryKill(instanceName: String, force: Boolean = true): Unit =
    perform(
      instanceName,
      {
        case x: Running =>
          noTracingLogger.info(s"Stopping external process for $instanceName (force=$force)")
          x.kill(force)
        case a: Configured =>
          ErrorUtil.internalError(
            new IllegalStateException(s"can not kill $instanceName as instance is not running")
          )
      },
    )

  def tryRestart(instanceName: String): Unit =
    perform(
      instanceName,
      {
        case x: Running =>
          noTracingLogger.info(s"Restarting external process for $instanceName")
          x.restart()
        case Configured(_, _, _, _) =>
          ErrorUtil.internalError(
            new IllegalStateException(s"can not kill $instanceName as instance is not running")
          )
      },
    )

  def tryInfo(instanceName: String): ProcessInfo =
    external
      .getOrElse(
        instanceName,
        ErrorUtil.internalError(new IllegalArgumentException(s"no such instance $instanceName")),
      )
      .info

  def exists(instanceName: String): Boolean = external.keySet.contains(instanceName)

  def killAndRemove(): Unit = {
    logger.info("Killing background processes due to shutdown")
    external.values.foreach {
      case Configured(_, _, _, _) => ()
      case Running(_, runner, _) =>
        runner.kill()
    }
    external.clear()
  }

  override def close(): Unit = killAndRemove()
}

class BackgroundRunner(
    val name: String,
    val command: Seq[String],
    val addEnvironment: Map[String, String],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    waitBeforeRestartMs: Int = 250,
) extends NamedLogging
    with FlagCloseable {

  import BackgroundRunner.*

  private def dumpOutputToLogger(parent: InputStream, level: Level): Unit = {
    @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
    class CopyOutput() extends NoTracing with Runnable {
      val buf = new StringWriter()

      override def run(): Unit =
        try {
          var b = parent.read()
          while (b != -1) {
            if (b == '\n' || buf.getBuffer.length() >= MaxLineLength) {
              // strip the ansi color commands from the string
              val msg = s"Output of $name: ${buf.toString}"
              level match {
                case Level.ERROR => logger.error(msg)
                case Level.WARN => logger.warn(msg)
                case Level.INFO => logger.info(msg)
                case Level.DEBUG => logger.debug(msg)
                case Level.TRACE => logger.trace(msg)
              }
              buf.getBuffer.setLength(0)
            } else { // if this if-condition is taken 2^30 times in a row, a buffer overflow error occurs
              buf.write(b)
            }
            b = parent.read()
          }
        } catch {
          case e: IOException =>
            logger.debug(s"External process was closed ${e.getMessage}")
          case e: BufferOverflowException =>
            logger.debug(
              "A BufferOverflowException occurred when writing to the external log file. " +
                "The cause is likely that there is a configuration error that leads to the external process to fail," +
                " and indefinitely output non-sense data to the output, leading to the buffer overflow exception. " +
                s"To find the root cause error, you will likely need to check the logs of the external process $name" +
                s"Error message of the exception: ${e.getMessage}"
            )
        }
    }
    val tr = new Thread(new CopyOutput(), s"output-copy-$name-$level")
    tr.setDaemon(true)
    tr.start()
  }

  private val pb = new ProcessBuilder(command.toList.asJava)

  pb.redirectOutput()
  pb.redirectErrorStream()
  addEnvironment.foreach { case (k, v) =>
    Option(pb.environment().put(k, v)) match {
      case Some(prev) => noTracingLogger.debug(s"Changed $k to $v from $prev")
      case None => noTracingLogger.debug(s"Set $k to $v")
    }
  }

  noTracingLogger.info(s"Starting command $name ${command.map(_.limit(160)).toString}")
  private val rt = pb.start()

  dumpOutputToLogger(rt.getInputStream, Level.DEBUG)
  dumpOutputToLogger(rt.getErrorStream, Level.INFO)

  def kill(force: Boolean = true): Unit =
    if (rt.isAlive) {
      try {
        if (!force) {
          noTracingLogger.debug(s"Killing process $name normally")
          val _ = rt.destroy()
        }
        if (
          force || !rt.waitFor(timeouts.shutdownProcessing.unwrap.toMillis, TimeUnit.MILLISECONDS)
        ) {
          noTracingLogger.info(s"Killing process $name forcibly")
          val _ = rt.destroyForcibly()
        }
      } catch {
        case ex: Throwable => noTracingLogger.error(s"Failed to kill forcibly: $command", ex)
      }
    } else {
      noTracingLogger.warn(s"Process $name is already shut down")
    }

  def restart(): BackgroundRunner = {
    kill()
    Threading.sleep(waitBeforeRestartMs.toLong)
    noTracingLogger.info(s"Restarting background runner with $command")
    new BackgroundRunner(name, command, addEnvironment, timeouts, loggerFactory)
  }

  override protected def onClosed(): Unit =
    if (rt.isAlive) {
      noTracingLogger.debug("Shutting down external process")
      rt.destroy()
    }

}

object BackgroundRunner {
  private val MaxLineLength = 8192
}

object BackgroundRunnerHelpers {

  /** Yields the jvm params specifying the current classpath, e.g., `Seq("-cp", myClassPath)`.
    * Excludes sbt dependencies.
    * @throws java.lang.IllegalStateException
    *   if the `java.class.path` property is unset and there is no file `classpath.txt` in the
    *   working directory.
    */
  def extractAppClassPathParams(): Seq[String] =
    loadIntelliJClasspath() match {
      case Some(cp) =>
        Seq("-cp", cp)
      case None =>
        val cpFile = tryGetAppClasspathFile()
        Seq(s"@${cpFile.name}")
    }

  private def loadIntelliJClasspath(): Option[String] =
    Some(System.getProperty("java.class.path")).filter(!_.matches(".*sbt-launch.*\\.jar"))

  private def tryGetAppClasspathFile(): File =
    tryGetFile(File(s"classpath.txt"), "dumpClassPath")

  private def tryGetTestClasspathFile(): File =
    tryGetFile(File(s"test.classpath.txt"), "dumpTestClassPath")

  private def tryGetFile(cpFile: File, command: String): File =
    if (cpFile.exists()) {
      cpFile
    } else {
      throw new IllegalStateException(
        s"Process is started using sbt, however you need to run `sbt $command` before running external processes."
      )
    }

  /** Yields a sequence with the elements of the current test classpath. Excludes sbt dependencies.
    * @throws java.lang.IllegalStateException
    *   if the `java.class.path` property is unset and there is no file `test.classpath.txt` in the
    *   working directory.
    */
  def extractTestClassPath(): Seq[String] =
    loadIntelliJClasspath() match {
      case Some(cp) => cp.split(":").toSeq
      case None =>
        tryGetTestClasspathFile().contentAsString.stripPrefix("-cp ").split(":").toSeq
    }

}
