// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.performance

import cats.syntax.foldable.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.{
  SequencerConnectionPoolDelays,
  SequencerConnections,
  SubmissionRequestAmplification,
  SynchronizerConnectionConfig,
}
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.performance.BasePerformanceIntegrationTest.toConnectivity
import com.digitalasset.canton.performance.PartyRole.{
  DvpIssuer,
  DvpTrader,
  Master,
  MasterDynamicConfig,
}
import com.digitalasset.canton.performance.model.java.orchestration.runtype
import com.digitalasset.canton.performance.{
  PerformanceRunner,
  PerformanceRunnerConfig,
  RateSettings,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.{BaseTest, SequencerAlias}
import monocle.macros.syntax.lens.*

import scala.concurrent.Future
import scala.concurrent.duration.*

class SequencerAggregationPerformanceIntegrationTest extends BasePerformanceIntegrationTest {
  setupPlugins(new UsePostgres(loggerFactory))

  private val masterName = "SAMaster"
  private val issuersPerNode: Int = 2
  private val tradersPerNode: Int = 4

  private def runnerConfigs(
      activeNodes: Seq[LocalParticipantReference]
  ): Seq[(PerformanceRunnerConfig, ParticipantReference)] = {
    val master = Master(
      masterName,
      runConfig = MasterDynamicConfig(
        totalCycles = 100, // Change this to increase the number of test iterations
        reportFrequency = 2,
        runType = new runtype.DvpRun(
          1000L, // change this to increase the number of assets per issuer,
          0,
          0,
          0,
        ),
      ),
    )
    activeNodes.zipWithIndex.map { case (node, index) =>
      val (syncId, other) =
        node.synchronizers.list_connected().map(_.physicalSynchronizerId).toList match {
          case fst :: rest => (fst, rest)
          case _ => throw new IllegalStateException("No synchronizers connected for " + node)
        }
      val issuers = (0 until issuersPerNode).map { ii =>
        DvpIssuer(s"Issuer$index-$ii", selfRegistrar = false, settings = RateSettings.defaults)
      }
      val traders = (0 until tradersPerNode).map { tt =>
        DvpTrader(s"Trader$index-$tt", settings = RateSettings.defaults)
      }
      val config = PerformanceRunnerConfig(
        master = masterName,
        localRoles = (traders ++ issuers ++ (if (index == 0) Seq(master) else Seq.empty)).toSet,
        ledger = toConnectivity(node),
        baseSynchronizerId = syncId,
        otherSynchronizers = other,
      )
      (config, node)
    }
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 5,
        numSequencers = 3,
        numMediators = 5,
      )
      .addConfigTransform(
        _.focus(_.parameters.timeouts.processing.shutdownProcessing)
          .replace(NonNegativeDuration.tryFromDuration(1.minute))
          .focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false)
      )
      .withSetup { implicit env =>
        import env.*

        // first, we need to bootstrap the synchronizer network
        // we use the first two sequencers as synchronizer owners
        val bootstrapper = new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            sequencers = env.sequencers.local,
            mediators = env.mediators.all,
            synchronizerOwners = Seq(sequencer1, sequencer2),
            synchronizerThreshold = PositiveInt.two,
            mediatorThreshold = env.mediators.all.size,
          )
        )
        bootstrapper.bootstrap()

        // connect the participants to all sequencers
        val connections = sequencers.local.map { reference =>
          reference.sequencerConnection.withAlias(SequencerAlias.tryCreate(reference.name))
        }
        participants.all.synchronizers.connect(
          SynchronizerConnectionConfig(
            daName,
            SequencerConnections.tryMany(
              connections,
              sequencerTrustThreshold = PositiveInt.tryCreate(sequencers.local.size - 1),
              sequencerLivenessMargin = NonNegativeInt.zero,
              SubmissionRequestAmplification.NoAmplification,
              SequencerConnectionPoolDelays.default,
            ),
          )
        )

        // upload the performance test dar to all participants
        participants.all.dars.upload(BaseTest.PerformanceTestPath)
      }

  // local variable so we can split the test into steps
  private val runnerConfig = new SingleUseCell[Seq[PerformanceRunnerConfig]]()

  "Run perf test with aggregation" in { implicit env =>
    import env.*

    val configs = runnerConfigs(Seq(participant1, participant2))
    runnerConfig.putIfAbsent(configs.map(_._1))
    PerformanceRunner.initializeRegistry(
      logger,
      configs,
      NonEmpty.mk(Seq, participant3, participant4, participant5),
    )
  }

  "verify bft settings" in { implicit env =>
    import env.*

    participant1.synchronizers.list_connected().map(_.physicalSynchronizerId).foreach { syncId =>
      val mediators =
        participant1.topology.mediators.list(synchronizerId = Some(syncId)).loneElement
      mediators.item.threshold.value shouldBe mediators.item.active.length
      mediators.item.threshold.value should be > 1

      // this is triggered by p3, so we don't know when it will be visible by p1
      eventually() {
        val registry =
          participant1.topology.party_to_participant_mappings
            .list(synchronizerId = syncId, filterParty = "registry::")
            .loneElement

        registry.item.threshold.value should be > 1
        registry.item.threshold.value shouldBe registry.item.participants.length
        registry.item.participants.map(_.permission).toSet shouldBe Set(
          ParticipantPermission.Confirmation
        )
      }
    }

  }

  "start performance runners on p1 and p2" in { implicit env =>
    import env.*

    logger.info("Starting performance runners on p1 and p2")
    val configs = runnerConfig.get.value
    val runners = configs.zipWithIndex.map { case (cfg, idx) =>
      val runner = new PerformanceRunner(
        cfg,
        _ => NoOpMetricsFactory,
        loggerFactory.append("runner", s"runner$idx"),
      )
      env.environment.addUserCloseable(runner)
      runner
    }

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val res = waitTimeout
          .await("waiting for per runners to complete")(
            Future.sequence(runners.map(r => r.startup()))
          )
          .sequence_
        res shouldBe Right(())

        // dump status
        runners.flatMap(_.status()).foreach { status =>
          logger.info(s"Driver status: $status")
        }

        // close runners
        Future.sequence(runners.map(_.closeF())).map(_ => ()).futureValue
      },
      forEvery(_)(acceptableLogMessage),
    )

  }

}
