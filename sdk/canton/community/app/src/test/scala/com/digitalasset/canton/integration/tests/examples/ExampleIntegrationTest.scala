// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.*
import com.digitalasset.canton.ConsoleScriptRunner
import com.digitalasset.canton.console.BufferedProcessLogger
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.{
  BaseIntegrationTest,
  ConfigTransform,
  EnvironmentDefinition,
  IsolatedEnvironments,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.util.ShowUtil.*

abstract class ExampleIntegrationTest(configPaths: File*)
    extends BaseIntegrationTest
    with IsolatedEnvironments
    with HasConsoleScriptRunner {

  protected def runAndAssertCommandSuccess(
      pb: scala.sys.process.ProcessBuilder,
      processLogger: BufferedProcessLogger,
  ): Unit = {
    val exitCode = pb.!(processLogger)
    if (exitCode != 0) {
      fail(s"Command failed:\n\n ${processLogger.output()}")
    }
  }

  protected def runAndAssertCommandFailure(
      pb: scala.sys.process.ProcessBuilder,
      processLogger: BufferedProcessLogger,
      expectedFailure: String,
  ): Unit = {
    val exitCode = pb.!(processLogger)
    if (exitCode == 0) {
      fail(s"Expected command failure but it succeeded")
    } else {
      processLogger.output() should include(expectedFailure)
    }
  }

  protected def additionalConfigTransform: Seq[ConfigTransform] =
    Seq.empty

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(configPaths*)
      .addConfigTransforms(additionalConfigTransform*)
}

trait HasConsoleScriptRunner { this: NamedLogging =>
  import org.scalatest.EitherValues.*

  def runScript(scriptPath: File)(implicit env: Environment): Unit =
    ConsoleScriptRunner.run(env, scriptPath.toJava, logger = logger).value.discard
}

object `ExampleIntegrationTest` {
  private val lock = new Mutex()
  lazy val communityApp = "community" / "app"
  lazy val communityAppSrc = "community" / "app" / "src"
  lazy val dockerImagesPath: File = "docker" / "canton" / "images"
  lazy val examplesPath: File = communityAppSrc / "pack" / "examples"
  lazy val simpleTopology: File = examplesPath / "01-simple-topology"
  lazy val referenceConfiguration: File = communityAppSrc / "pack" / "config"
  lazy val composabilityConfiguration: File = examplesPath / "05-composability"
  lazy val repairConfiguration: File = examplesPath / "07-repair"
  lazy val interactiveSubmissionFolder: File = examplesPath / "08-interactive-submission"

  object JsonApiExample {
    lazy val path = examplesPath / "09-json-api"
    lazy val codegenOutput = communityApp / "target" / "test-daml-codegen-ts"
  }

  lazy val advancedConfTestEnv: File =
    communityAppSrc / "test" / "resources" / "advancedConfDef.env"
  lazy val bftSequencerConfigurationFolder: File = examplesPath / "11-bft-sequencer"

  def ensureSystemProperties(kvs: (String, String)*): Unit = (lock.exclusive {
    kvs.foreach { case (key, value) =>
      Option(System.getProperty(key)) match {
        case Some(oldValue) =>
          require(
            oldValue == value,
            show"Trying to set incompatible system properties for ${key.singleQuoted}. Old: ${oldValue.doubleQuoted}, new: ${value.doubleQuoted}.",
          )
        case None =>
          System.setProperty(key, value)
      }
    }
  })
}
