// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.connection

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.InstanceReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseExternalProcess,
  UsePostgres,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.sequencing.{
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.{SequencerAlias, config}
import org.scalatest.Assertion

import scala.concurrent.duration.DurationInt

/** This test checks that the sequencer connection pool properly recovers connections and
  * subscriptions when sequencers go down.
  *
  * The environment is as follows:
  *   - 2 participants
  *   - 4 sequencers (running in external processes)
  *   - 1 mediator
  *
  * The participants and the mediator connect to all the sequencers with a trust threshold of 2 and
  * a liveness margin of 0. When they start up, all sequencers are up, which means that they will
  * have connections to all of them for sending, but only two subscriptions on random sequencers
  * will be started (since the liveness margin is 0).
  *
  * The test then executes the following scenario:
  *
  *   - Ping from participant 1 to participant 2
  *   - Stop sequencers 3 and 4
  *   - Ping again (nodes must acquire subscriptions on sequencers 1 and 2 if they did not yet have
  *     them)
  *   - Restart sequencers 3 and 4, stop sequencers 1 and 2
  *   - Ping again (nodes must acquire subscriptions on sequencers 3 and 4)
  *   - Restart sequencers 1 and 2
  *
  * This scenario is executed once using a graceful stop to bring the sequencers down, and a second
  * time using a kill. This checks that connection failures are detected properly even when they are
  * not gracefully closed.
  */
sealed trait BftSequencerConnectionsIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  protected lazy val externalPlugin = new UseExternalProcess(
    loggerFactory,
    externalSequencers = Set("sequencer1", "sequencer2", "sequencer3", "sequencer4"),
    fileNameHint = this.getClass.getSimpleName,
  )

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S4M1_Config
      .addConfigTransforms(
        ConfigTransforms.setConnectionPool(true),
        // Increase the acknowledgement interval to avoid flakes with failing acknowledgements log messages
        ConfigTransforms.updateSequencerClientAcknowledgementInterval(
          NonNegativeFiniteDuration.tryOfMinutes(60)
        ),
      )
      .withManualStart
      .withSetup { implicit env =>
        import env.*

        sequencers.remote.foreach { seq =>
          logger.debug(s"Starting sequencer ${seq.name}")
          externalPlugin.start(seq.name)
        }
        sequencers.remote.foreach { seq =>
          seq.health.wait_for_running()
          logger.debug(s"Sequencer ${seq.name} is running")
        }
      }
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](remoteSequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = sequencers.remote,
            mediators = Seq(mediator1),
            overrideMediatorToSequencers = Some(
              Map(
                mediator1 -> (sequencers.remote,
                /* trust threshold */ PositiveInt.two, /* liveness margin */ NonNegativeInt.zero)
              )
            ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        sequencers.remote.foreach { seq =>
          seq.health.wait_for_initialized()
          logger.debug(s"Sequencer ${seq.name} is initialized")
        }
      }

  private lazy val expectedLogEntries = Seq[LogEntry => Assertion](
    _.warningMessage should include regex
      raw"Request failed for server-.*\. Is the server running\? Did you configure the server address as 0\.0\.0\.0\?" +
      raw" Are you using the right TLS settings\?",
    _.errorMessage should (include regex
      raw"Request failed for server-.*\." and include("GrpcServerError: UNKNOWN/channel closed")),
  )

  private def pingWithSequencersDown()(implicit env: TestConsoleEnvironment) = {
    import env.*

    val pingTimeout = config.NonNegativeDuration.ofSeconds(120)

    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      participant1.health.ping(participant2.id, timeout = pingTimeout),
      LogEntry.assertLogSeq(
        mustContainWithClue = Seq.empty,
        mayContain = expectedLogEntries,
      ),
    )
  }

  "BFT Synchronizer" must {
    "Initialize the setup" in { implicit env =>
      import env.*

      val amplification = SubmissionRequestAmplification(
        factor = 10,
        patience = config.NonNegativeFiniteDuration.tryFromDuration(5.seconds),
      )

      clue("reconfigure mediator to use amplification") {
        mediator1.sequencer_connection.modify_connections { old =>
          SequencerConnections.tryMany(
            connections = old.connections,
            sequencerTrustThreshold = old.sequencerTrustThreshold,
            sequencerLivenessMargin = old.sequencerLivenessMargin,
            submissionRequestAmplification = amplification,
            sequencerConnectionPoolDelays = old.sequencerConnectionPoolDelays,
          )
        }
      }

      val connectionsConfig = sequencers.remote.map(s =>
        s.config.publicApi.asSequencerConnection(SequencerAlias.tryCreate(s.name))
      )

      clue("connect participant1 to all sequencers") {
        participant1.start()
        participant1.synchronizers.connect_bft(
          connections = connectionsConfig,
          sequencerTrustThreshold = PositiveInt.two,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplification,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
          validation = SequencerConnectionValidation.Disabled,
        )
      }

      clue("connect participant2 to all sequencers") {
        participant2.start()
        participant2.synchronizers.connect_bft(
          connections = connectionsConfig,
          sequencerTrustThreshold = PositiveInt.two,
          sequencerLivenessMargin = NonNegativeInt.zero,
          submissionRequestAmplification = amplification,
          synchronizerAlias = daName,
          physicalSynchronizerId = Some(daId),
          validation = SequencerConnectionValidation.Disabled,
        )
      }
    }

    "Handle sequencers stopped graciously" in { implicit env =>
      handleFailingSequencers(forceKill = false)
    }

    "Restart sequencers 1 and 2" in { implicit env =>
      import env.*

      Seq(remoteSequencer1, remoteSequencer2).foreach(seq => externalPlugin.start(seq.name))
    }

    "Handle killed sequencers" in { implicit env =>
      handleFailingSequencers(forceKill = true)
    }
  }

  private def handleFailingSequencers(
      forceKill: Boolean
  )(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    clue("Perform an initial ping") {
      participant1.health.ping(participant2.id)
    }

    clue("Stop sequencers 3 and 4") {
      Seq(remoteSequencer3, remoteSequencer4).foreach(seq =>
        externalPlugin.kill(seq.name, force = forceKill)
      )
    }
    clue("Ping again") {
      pingWithSequencersDown()
    }

    clue("Restart sequencers 3 and 4, stop sequencers 1 and 2") {
      Seq(remoteSequencer3, remoteSequencer4).foreach(seq => externalPlugin.start(seq.name))
      Seq(remoteSequencer1, remoteSequencer2).foreach(seq =>
        externalPlugin.kill(seq.name, force = forceKill)
      )
    }
    clue("Ping yet again") {
      pingWithSequencersDown()
    }
  }
}

class BftSequencerConnectionsIntegrationTestDefault extends BftSequencerConnectionsIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
  registerPlugin(externalPlugin)
}
