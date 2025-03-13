// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import cats.syntax.functorFilter.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.*
import com.digitalasset.canton.integration.plugins.UseExternalProcess.ShutdownPhase
import com.digitalasset.canton.integration.util.{BackgroundRunnerHandler, BackgroundRunnerHelpers}
import com.digitalasset.canton.integration.{EnvironmentSetupPlugin, TestConsoleEnvironment}
import com.digitalasset.canton.logging.NamedLogging

/** Run Canton processes in separate JVM instances such that we can kill them at any time
  *
  * The implementation of this trait that you will likely want to use is
  * [[com.digitalasset.canton.integration.plugins.UseExternalProcess]].
  */
trait UseExternalProcessBase[Config] extends EnvironmentSetupPlugin with NamedLogging {

  /** A short hint added to the files name to make it easier to identify what they belong to. */
  def fileNameHint: String

  /** When should the external processes be killed relative to the main test environment */
  def shutdownPhase: ShutdownPhase

  /** The address of a clock server in case of static time. */
  def clockServer: Option[ClientConfig]

  protected val handler: BackgroundRunnerHandler[Config] =
    new BackgroundRunnerHandler[Config](
      DefaultProcessingTimeouts.testing,
      loggerFactory,
    )

  protected val threadingProps: Seq[String] =
    Threading.threadingProps.mapFilter(prop =>
      Option(System.getProperty(prop)).map(value => s"-D$prop=$value")
    )

  // Using same logging as for the test suite.
  // Particularly important to immediately flush logs so that log is flushed before the process gets killed.
  protected val loggingProps: Seq[String] =
    Seq(
      "-Dlogback.configurationFile=logback-test.xml"
    )

  protected val classpathProps: Seq[String] = BackgroundRunnerHelpers.extractAppClassPathParams()

  protected def cantonInvocationArgs(
      logFile: String,
      configFile: String,
      extraArgs: Seq[String] = Seq(),
  ): Seq[String] =
    Seq(
      "daemon",
      "-v",
      s"--log-file-name=$logFile",
      "-c",
      configFile,
    ) ++ extraArgs

  override def beforeEnvironmentDestroyed(
      environment: TestConsoleEnvironment
  ): Unit =
    if (shutdownPhase == ShutdownPhase.BeforeEnvironment) handler.killAndRemove()

  override def afterEnvironmentDestroyed(config: CantonConfig): Unit =
    if (shutdownPhase == ShutdownPhase.AfterEnvironment) handler.killAndRemove()

  def kill(instanceName: String, force: Boolean = true): Unit = handler.tryKill(instanceName, force)
  def stop(instanceName: String): Unit = handler.stopAndRemove(instanceName)
  def isRunning(instanceName: String): Boolean = handler.tryIsRunning(instanceName)
  def restart(instanceName: String): Unit = handler.tryRestart(instanceName)
  def config(instanceName: String): Config = handler.tryInfo(instanceName)
}
