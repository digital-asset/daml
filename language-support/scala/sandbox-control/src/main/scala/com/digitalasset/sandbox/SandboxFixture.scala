// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.sandbox

import java.io.File
import java.nio.file.{Path, Paths}

import org.apache.commons.io.FileUtils
import com.digitalasset.testing.BuildSystemSupport
import com.typesafe.scalalogging.LazyLogging

object SandboxFixture {
  private val ghcPrimDar = new File(
    "daml-foundations/daml-ghc/package-database/deprecated/daml-prim.dar")

  class SandboxContext(
      val host: String,
      val port: Int,
      val dar: File,
      sandboxJar: File,
      logbackConfig: Option[File],
      extraArgs: List[String])
      extends LazyLogging {

    import SandboxContext._

    import sys.process._

    require(dar.exists, s"DAR file does not exist: $dar")
    logbackConfig.foreach(lc =>
      require(lc.exists, s"Logback config does not exist: $logbackConfig"))

    private var sandboxProcess: Option[Process] = None

    def start(): Unit = {
      val javaCommand = {
        val javaHome = System.getProperty("java.home")
        Paths.get(javaHome, "bin", "java")
      }

      val sandboxCommand = s"-jar $sandboxJar" :: sandboxParameters(port, dar) ++ extraArgs

      val command = logbackConfig.fold(javaCommand :: sandboxCommand) { lc =>
        val logbackParam = s"-Dlogback.configurationFile=${lc.getAbsolutePath}"
        javaCommand :: logbackParam :: sandboxCommand
      }

      logger.info("Starting sandbox process: {}", command)
      sandboxProcess = Some(Process(command.mkString(" ")).run())
    }

    def shutdown(): Unit = {
      logger.info("Shutting down sandbox process: {}", sandboxProcess)
      sandboxProcess.foreach(_.destroy())
      sandboxProcess = None
    }

  }

  object SandboxContext {

    def apply(
        port: Int,
        dar: File = TEST_ALL_DAR,
        sbtConfig: String = "it",
        extraArgs: List[String] = List.empty): SandboxContext =
      new SandboxContext(
        "127.0.0.1",
        port,
        dar,
        locateSandboxJar,
        Some(logbackConfig(sbtConfig)),
        extraArgs)

    val TEST_ALL_DAR =
      BuildSystemSupport
        .path(
          "target/scala-2.12/resource_managed/it/dars/com/digitalasset/sample/test-all/0.1/test-all-0.1.dar"
        )
        .toFile
        .getAbsoluteFile

    def defaultGhcPrimDalf: File =
      Option(getClass getResource "/da-hs-damlc-app/resources/package-db/gen/daml-prim.dalf")
        .map { ghcPrimUrl =>
          val res = File createTempFile ("daml-prim", ".dalf")
          FileUtils copyURLToFile (ghcPrimUrl, res)
          res
        }
        .getOrElse {
          throw new IllegalStateException("No daml-prim.dalf available in classpath")
        }

    private def locateSandboxJar: File = {
      val jarKey = "com.digitalasset.sandbox.jar"
      Option(System.getProperty(jarKey))
        .map { path =>
          val sandboxJar = new File(path).getAbsoluteFile
          require(sandboxJar.exists, s"Sandbox JAR does not exist: $sandboxJar")
          sandboxJar
        }
        .getOrElse(throw new IllegalStateException(
          s"Cannot start Sandbox, '$jarKey' system property is not set"))
    }

    def logbackConfig(sbtConfig: String) =
      BuildSystemSupport.path(s"src/$sbtConfig/resources/logback-sandbox.xml").toFile

    def sandboxParameters(port: Int, dar: File): List[String] =
      List(s"--port $port", "--wall-clock-time", ghcPrimDar.getAbsolutePath, dar.getAbsolutePath)
  }

  private def run[T](sandboxContext: SandboxContext, testCode: SandboxContext => T): T =
    try {
      sandboxContext.start()
      testCode(sandboxContext)
    } finally {
      sandboxContext.shutdown()
    }

  def withSandbox[T](dar: File, port: Int, sbtConfig: String = "it")(
      testCode: SandboxContext => T): T = run(SandboxContext(port, dar, sbtConfig), testCode)

  def withSecureSandbox[T](
      port: Int,
      serverCertChain: Path,
      serverPrivateKey: Path,
      trustCertCollection: Path)(testCode: SandboxContext => T): T = {
    val sslArgs = List(
      "--pem",
      serverPrivateKey.toString,
      "--crt",
      serverCertChain.toString,
      "--cacrt",
      trustCertCollection.toString
    )

    run(SandboxContext(port, extraArgs = sslArgs), testCode)
  }

}
