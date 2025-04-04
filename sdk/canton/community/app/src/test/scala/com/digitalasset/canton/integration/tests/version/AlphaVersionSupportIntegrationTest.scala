// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.version

import com.digitalasset.canton.admin.api.client.data.StaticSynchronizerParameters
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, StorageConfig}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  LocalParticipantReference,
  LocalSequencerReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UseH2,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInconsistentConnectivity
import com.digitalasset.canton.resource.CommunityStorageFactory
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.store.InitializationStore
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ParticipantProtocolVersion, ProtocolVersion}
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion

import java.time.Duration
import scala.concurrent.{ExecutionContext, Future}

sealed trait AlphaVersionSupportIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with FlagCloseable
    with HasCloseContext {

  protected val sequencerGroups: MultiSynchronizer = MultiSynchronizer(
    Seq(
      Set(InstanceName.tryCreate("sequencer1")),
      Set(InstanceName.tryCreate("sequencer2")),
    )
  )

  /*
    This test has 2 Canton environments:
     - referenced as `alphaEnv` (passed via fixture into tests):
       has `da` and `participant1` with alpha features support enabled
       synchronizer nodes: sequencer1, mediator1
       alias: da
     - referenced as `prodEnv` (created with manualCreateEnvironment, passed via the test class field):
       has `acme` and `participant2` with no alpha features allowed
       synchronizer nodes: sequencer2, mediator2
       alias: acme
   */
  override lazy val environmentDefinition: EnvironmentDefinition = {
    val minimumStableProtocolVersion =
      if (testedProtocolVersion.isAlpha)
        ProtocolVersion.latest
      else testedProtocolVersion

    EnvironmentDefinition.P2S2M2_Manual
      .addConfigTransform(ConfigTransforms.enableNonStandardConfig)
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") {
        // set on `participant1` a minimum-pv and enable alpha version support
        _.focus(_.parameters.minimumProtocolVersion)
          .replace(Some(ParticipantProtocolVersion(minimumStableProtocolVersion)))
          .focus(_.parameters.alphaVersionSupport)
          .replace(true)
          .focus(_.sequencerClient.warnDisconnectDelay)
          .replace(config.NonNegativeFiniteDuration(Duration.ofSeconds(30)))
      })
      .addConfigTransform(ConfigTransforms.updateSequencerConfig("sequencer1") {
        _.focus(_.parameters.alphaVersionSupport).replace(true)
      })
      .addConfigTransform(ConfigTransforms.updateMediatorConfig("mediator1") {
        _.focus(_.parameters.alphaVersionSupport).replace(true)
      })
      .withSetup { implicit env =>
        import env.*

        participants.local.start()
        sequencer1.start()
        sequencer2.start()
        mediator1.start()
        mediator2.start()

        bootstrap.synchronizer(
          daName.unwrap,
          sequencers = Seq(sequencer1),
          mediators = Seq(mediator1),
          synchronizerOwners = Seq(sequencer1),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters =
            StaticSynchronizerParameters.defaults(sequencer1.config.crypto, ProtocolVersion.dev),
        )

        bootstrap.synchronizer(
          acmeName.unwrap,
          sequencers = Seq(sequencer2),
          mediators = Seq(mediator2),
          synchronizerOwners = Seq(sequencer2),
          synchronizerThreshold = PositiveInt.one,
          staticSynchronizerParameters = EnvironmentDefinition.defaultStaticSynchronizerParameters,
        )
      }
  }

  private def checkSequencerIsAlpha(
      env: ConsoleEnvironment,
      node: LocalSequencerReference,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Boolean] = {
    val iname = InstanceName.tryCreate(node.name)
    val config =
      env.environment.config.sequencers.get(iname).valueOrFail(s"$iname should be there").storage
    checkIsDev(config)
  }

  private def checkParticipantIsDev(
      env: ConsoleEnvironment,
      node: LocalParticipantReference,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Boolean] = {
    val instanceName = InstanceName.tryCreate(node.name)
    val config = env.environment.config.participants
      .get(instanceName)
      .valueOrFail(s"$instanceName should be there")
      .storage
    checkIsDev(config)
  }

  private def checkIsDev(
      config: StorageConfig
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Boolean] = {
    val storageFactory = new CommunityStorageFactory(config)
    val storage = storageFactory
      .create(
        connectionPoolForParticipant = false,
        logQueryCost = None,
        clock = new SimClock(CantonTimestamp.Epoch, loggerFactory),
        scheduler = None,
        metrics = CommonMockMetrics.dbStorage,
        timeouts = timeouts,
        loggerFactory,
      )
      .valueOrFail("should be there")
      .onShutdown(fail("no shutdown"))

    val initializationStore = InitializationStore(storage, timeouts, loggerFactory)
    initializationStore.throwIfNotDev.transform { res =>
      storage.close()
      res
    }
  }.failOnShutdown

  private def expectedLogErrors(message: String): Assertion =
    message should (
      include("does not exist") // postgres
        or
          include("not found") // H2
    )

  "synchronizers" should {
    "with alpha version have extra migration" in { alphaEnv =>
      import alphaEnv.*

      checkSequencerIsAlpha(alphaEnv, alphaEnv.sequencer1).futureValue shouldBe true
    }

    "without dev version have no extra migration" in { env =>
      implicit val ec: ExecutionContext = env.executionContext

      if (testedProtocolVersion.isStable)
        expectedLogErrors(
          checkSequencerIsAlpha(env, env.sequencer2).failed.futureValue.getMessage
        )
      else // if everyone is on dev, no error
        checkSequencerIsAlpha(env, env.sequencer2).futureValue shouldBe true
    }
  }

  // If testedProtocolVersion is alpha, then the participants are also using an alpha protocol version. In that case, this test is wrong.
  if (testedProtocolVersion.isStable) {
    "participant without dev version" should {
      "have no extra migration" in { implicit env =>
        implicit val ec: ExecutionContext = env.executionContext
        expectedLogErrors(
          checkParticipantIsDev(env, env.participant2).failed.futureValue.getMessage
        )
      }

      "not be able to connect to a synchronizer with dev" in { env =>
        assertThrowsAndLogsCommandFailures(
          env.participant2.synchronizers.connect(env.sequencer1, env.daName),
          entry => {
            entry.shouldBeCantonErrorCode(SyncServiceInconsistentConnectivity)
            entry.message should include(
              s"The protocol version required by the server (${ProtocolVersion.dev}) is not among the supported protocol versions by the client"
            )
          },
        )
      }

      "succeed subsequently on normal synchronizer" in { env =>
        env.participant2.synchronizers
          .connect_local(env.sequencer2, alias = env.acmeName)
      }
    }
  }

  "participant with dev version" should {
    "has extra migration" in { alphaEnv =>
      import alphaEnv.*

      checkParticipantIsDev(alphaEnv, alphaEnv.participant1).futureValue shouldBe true
    }

    "connect to a synchronizer with dev" in { alphaEnv =>
      alphaEnv.participant1.synchronizers
        .connect_local(alphaEnv.sequencer1, alias = alphaEnv.daName)
      alphaEnv.participant1.health.ping(alphaEnv.participant1.id)
    }

    "also succeed on normal synchronizer" in { env =>
      import env.*

      participant1.synchronizers.connect(sequencer2, acmeName)
      participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant2.health.ping(participant1.id)
    }
  }
}

class AlphaVersionSupportIntegrationTestH2 extends AlphaVersionSupportIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.H2](
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}

class AlphaVersionSupportIntegrationTestPostgres extends AlphaVersionSupportIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = sequencerGroups,
    )
  )
}
