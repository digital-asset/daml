// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.test.runner

import java.io.File

import com.daml.navigator.test.runner.Runner.LazyProcessLogger
import com.typesafe.scalalogging.LazyLogging

/**
  * Run a packaged version of the Sandbox.
  * Update the project dependencies to change the sandbox version.
  */
object PackagedSandbox {

  class SandboxContext(
      val host: String,
      val port: Int,
      val dars: List[File],
      logbackConfig: File,
      scenario: String)
      extends LazyLogging {

    import sys.process._

    require(logbackConfig.exists, s"Logback config does not exist: $logbackConfig")

    private val sandboxJar = locateSandboxJar()
    private var sandboxProcess: Option[Process] = None

    private def locateSandboxJar(): File = {
      val jarKey = "com.daml.sandbox.jar"
      Option(System.getProperty(jarKey))
        .map { path =>
          val sandboxJar = new File(path).getAbsoluteFile
          require(sandboxJar.exists, s"Sandbox JAR does not exist: $sandboxJar")
          sandboxJar
        }
        .getOrElse(throw new IllegalStateException(
          s"Cannot start Sandbox, '$jarKey' system property is not set"))
    }

    def start(): Unit = {
      val command = List(
        "java",
        // s"-Dlogback.configurationFile=${logbackConfig.getAbsolutePath}",
        "-jar",
        sandboxJar.toString,
        "--port",
        s"$port",
        "--scenario",
        scenario
      ) ++ dars.map(_.toString)

      sandboxProcess = Some(Runner.executeAsync(command, Some(new LazyProcessLogger("[sandbox] "))))
    }

    def shutdown(): Unit = {
      if (sandboxProcess.exists(_.isAlive())) {
        logger.info("Shutting down sandbox process")
        sandboxProcess.foreach(_.destroy())
        sandboxProcess = None
      }
    }
  }

  object SandboxContext {

    def apply(
        port: Int,
        dars: List[File],
        sbtConfig: String = "it",
        scenario: String): SandboxContext =
      new SandboxContext("127.0.0.1", port, dars, logbackConfig(sbtConfig), scenario)

    def logbackConfig(sbtConfig: String) = new File(s"src/$sbtConfig/resources/logback-sandbox.xml")

  }

  def runAsync(port: Int, dars: List[File], scenario: String): Unit => Unit = {
    val context = SandboxContext(port, dars, "main", scenario)
    context.start()

    sys addShutdownHook context.shutdown()
    _ =>
      context.shutdown()
  }

}
