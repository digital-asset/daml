// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import better.files.*
import com.daml.metrics.api.testing.InMemoryMetricsFactory
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CantonConfig,
  DbConfig,
  EnterpriseCantonEdition,
  TestingConfigInternal,
}
import com.digitalasset.canton.console.{LocalInstanceReference, RemoteInstanceReference}
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.referenceConfiguration
import com.digitalasset.canton.metrics.MetricsFactoryType
import com.digitalasset.canton.util.ResourceUtil
import monocle.macros.syntax.lens.*

class ReferenceConfigSandboxExampleIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(Set("local")),
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(
        "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env",
        referenceConfiguration / "sandbox.conf",
      )
      .clearConfigTransforms()
      // Disable the cache to avoid spurious compile errors.
      .addConfigTransform(_.focus(_.parameters.console.cacheDir).replace(None))
      .addConfigTransforms(ConfigTransforms.setProtocolVersion(testedProtocolVersion)*)

  "sandbox" should {
    "successfully connect" in { implicit env =>
      import env.*

      val sandbox = p("sandbox")
      val sequencer = s("local")
      val mediator = m("localMediator")
      clue("bootstrap") {
        bootstrap.synchronizer(
          "sandbox",
          Seq(sequencer),
          Seq(mediator),
          Seq(sequencer),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
      }
      clue("connect") {
        sandbox.synchronizers.connect_local(sequencer, daName)
      }
      clue("ping") {
        sandbox.health.maybe_ping(sandbox) should not be empty
      }
    }
  }

}

// NOTE: This test does not work locally unless you configure postgres environment variables such as POSTGRES_USER.
// The values from the UsePostgres script are not adopted by the external reference config.
class ReferenceConfigExampleIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasExecutionContext {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer.tryCreate(Set("sequencer")),
    )
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .fromFiles(
        "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env",
        referenceConfiguration / "misc" / "debug.conf",
        referenceConfiguration / "misc" / "dev.conf",
        referenceConfiguration / "misc" / "low-latency-sequencer.conf",
        referenceConfiguration / "participant.conf",
        referenceConfiguration / "mediator.conf",
        referenceConfiguration / "sequencer.conf",
      )
      .clearConfigTransforms()
      // Disable the cache to avoid spurious compile errors.
      .addConfigTransform(_.focus(_.parameters.console.cacheDir).replace(None))
      .addConfigTransforms(ConfigTransforms.setProtocolVersion(testedProtocolVersion)*)

  protected def environmentFor(
      files: List[better.files.File],
      transform: ConfigTransform = ConfigTransforms.identity,
  ) = {
    val environmentDefinition = EnvironmentDefinition
      .fromFiles(files*)
      .clearConfigTransforms()
      .addConfigTransform(transform)

    val config = environmentDefinition.generateConfig
    val env = environmentFactory.create(
      config,
      loggerFactory = loggerFactory,
      testingConfigInternal = TestingConfigInternal(
        metricsFactoryType = MetricsFactoryType.InMemory(_ => new InMemoryMetricsFactory),
        initializeGlobalOpenTelemetry = false,
      ),
    )
    environmentDefinition.createTestConsole(env, loggerFactory)

  }

  "participant" should {
    "successfully have started up" in { implicit env =>
      import env.*
      val participant = lp("participant")
      participant.config.adminApi.tls should not be empty
      participant.config.ledgerApi.tls should not be empty
      participant.health.status.trySuccess.active shouldBe true
      participant.ledger_api.state.acs.of_all() shouldBe empty
    }
  }

  "distributed synchronizer" should {
    "should expose tls correctly" in { implicit env =>
      import env.*
      val mediator = lm("mediator")
      val sequencer = ls("sequencer")
      Seq[LocalInstanceReference](mediator, sequencer).foreach { node =>
        node.config.adminApi.tls should not be empty
      }
      sequencer.config.publicApi.tls should not be empty
    }
  }

  "remote configs" should {
    "allow to connect to nodes and bootstrap synchronizer" in { _ =>
      ResourceUtil.withResource(
        environmentFor(
          List(
            "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env",
            referenceConfiguration / "misc" / "dev.conf",
            referenceConfiguration / "remote" / "participant.conf",
            referenceConfiguration / "remote" / "mediator.conf",
            referenceConfiguration / "remote" / "sequencer.conf",
          )
        )
      ) { implicit env =>
        import env.*
        val participant = rp("participant")
        val mediator = rm("mediator")
        val sequencer = rs("sequencer")

        // bootstrap via remote nodes. if it works using remote nodes, it will anyway work
        // using local nodes ...
        bootstrap.synchronizer(
          "synchronizer",
          Seq(sequencer),
          Seq(mediator),
          Seq(sequencer),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
        // check status
        Seq[RemoteInstanceReference](participant, mediator, sequencer).foreach { node =>
          node.health.status.trySuccess.active shouldBe true
        }
      }
    }
  }

  "distributed synchronizer" should {

    "connect the participant" in { implicit env =>
      import env.*
      val participant = lp("participant")
      val sequencer = s("sequencer")
      participant.synchronizers.connect_local(sequencer, daName)
      participant.health.maybe_ping(participant) should not be empty
    }
  }

}

class AdvancedConfigFilesTest extends BaseTestWordSpec {
  "parse the additional config scripts of advanced config" in {
    // just testing that the config still parses
    def testConfig(config: Seq[String]) = {
      val files = config.map(file => (referenceConfiguration / file).toJava)
      CantonConfig.parseAndLoadOrExit(files, EnterpriseCantonEdition)
    }
    testConfig(
      Seq(
        "monitoring/tracing.conf",
        "sandbox.conf",
      )
    )

  }
}
