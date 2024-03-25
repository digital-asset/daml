// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files.*
import com.digitalasset.canton.config.CommunityDbConfig.H2
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  IsolatedCommunityEnvironments,
}
import com.digitalasset.canton.integration.plugins.UseCommunityReferenceBlockSequencer
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.{
  ensureSystemProperties,
  referenceConfiguration,
  repairConfiguration,
  simpleTopology,
}
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{ConsoleScriptRunner, DiscardOps}
import monocle.macros.syntax.lens.*

import scala.concurrent.blocking

abstract class ExampleIntegrationTest(configPaths: File*)
    extends CommunityIntegrationTest
    with IsolatedCommunityEnvironments
    with HasConsoleScriptRunner {

  override lazy val environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition
      .fromFiles(configPaths*)
      .addConfigTransforms(
        // lets not share databases
        CommunityConfigTransforms.uniqueH2DatabaseNames,
        _.focus(_.monitoring.tracing.propagation).replace(TracingConfig.Propagation.Enabled),
        CommunityConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
          // to make sure that the picked up time for the snapshot is the most recent one
          config
            .focus(_.parameters.transferTimeProofFreshnessProportion)
            .replace(NonNegativeInt.zero)
        },
        CommunityConfigTransforms.uniquePorts,
      )
}

trait HasConsoleScriptRunner { this: NamedLogging =>
  import org.scalatest.EitherValues.*
  def runScript(scriptPath: File)(implicit env: Environment): Unit =
    ConsoleScriptRunner.run(env, scriptPath.toJava, logger = logger).value.discard
}

object ExampleIntegrationTest {
  lazy val examplesPath: File = "community" / "app" / "src" / "pack" / "examples"
  lazy val simpleTopology: File = examplesPath / "01-simple-topology"
  lazy val referenceConfiguration: File = "community" / "app" / "src" / "pack" / "config"
  lazy val composabilityConfiguration: File = examplesPath / "05-composability"
  lazy val repairConfiguration: File = examplesPath / "07-repair"
  lazy val advancedConfTestEnv: File =
    "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env"

  def ensureSystemProperties(kvs: (String, String)*): Unit = blocking(synchronized {
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

sealed abstract class SimplePingExampleXIntegrationTest
    extends ExampleIntegrationTest(simpleTopology / "simple-topology.conf") {

  "run simple-ping.canton successfully" in { implicit env =>
    import env.*
    val port = sequencer1.sequencerConnection.endpoints.head.port.unwrap.toString
    ensureSystemProperties(("canton-examples.da-port", port))
    runScript(simpleTopology / "simple-ping.canton")(environment)
  }
}

final class SimplePingExampleReferenceXIntegrationTestDefault
    extends SimplePingExampleXIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[H2](loggerFactory))
}

sealed abstract class RepairExampleXIntegrationTest
    extends ExampleIntegrationTest(
      referenceConfiguration / "storage" / "h2.conf",
      repairConfiguration / "domain-repair-lost.conf",
      repairConfiguration / "domain-repair-new.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "enable-preview-commands.conf",
    ) {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "domain-repair-init.canton")(env.environment)
  }
}

final class RepairExampleReferenceXIntegrationTestDefault extends RepairExampleXIntegrationTest {
  registerPlugin(new UseCommunityReferenceBlockSequencer[H2](loggerFactory))
}
