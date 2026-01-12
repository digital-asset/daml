// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.{
  AdverseScenario,
  Component,
  ReliabilityTest,
  Remediation,
}
import com.digitalasset.canton.config.PositiveDurationSeconds
import com.digitalasset.canton.console.SequencerReference
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import eu.rekawek.toxiproxy.model.ToxicDirection

import scala.concurrent.duration.*

trait HorizontallyScaledSequencerDatabaseFaultTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  def proxyConf(sequencerName: String): ProxyConfig

  lazy val toxiProxyConf =
    UseToxiproxy.ToxiproxyConfig(List(proxyConf("sequencer2"), proxyConf("sequencer3")))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  def waitUntilInactive(sequencer: SequencerReference): Unit =
    eventually(40.seconds) {
      withClue("sequencer component should not be active") {
        sequencer.health.status.trySuccess.sequencer.isActive shouldBe false
      }
    }

  def waitUntilActive(sequencer: SequencerReference): Unit =
    eventually(40.seconds) {
      withClue("sequencer component should be active") {
        sequencer.health.status.trySuccess.sequencer.isActive shouldBe true
      }
    }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 3,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        NetworkBootstrapper(Seq(EnvironmentDefinition.S1M1))
      }
      .withSetup { implicit env =>
        import env.*
        runOnAllInitializedSynchronizersForAllOwners((owner, synchronizer) =>
          owner.topology.synchronizer_parameters
            .propose_update(
              synchronizer.synchronizerId,
              _.update(reconciliationInterval = PositiveDurationSeconds.ofSeconds(1)),
            )
        )
        participant1.synchronizers.connect_multi(daName, Seq(sequencer2, sequencer3))
      }
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

  protected val getToxiProxy: String => RunningProxy

  protected def setupPlugins(storagePlugins: EnvironmentSetupPlugin*): String => RunningProxy = {
    storagePlugins.foreach(registerPlugin)
    registerPlugin(
      UseSharedStorage.forSequencers("sequencer1", Seq("sequencer2", "sequencer3"), loggerFactory)
    )

    // Configure toxiproxy only after the shared storage has been configured
    registerPlugin(toxiproxyPlugin)

    // Return a function that returns the running proxy during tests
    sequencerName =>
      toxiproxyPlugin.runningToxiproxy
        .getProxy(toxiProxyConf.proxies.find(_.name.contains(sequencerName)).value.name)
        .value
  }

}

trait RunningSequencerRecoveryOnDatabaseFaultTest
    extends HorizontallyScaledSequencerDatabaseFaultTest {
  "can ping".taggedAs(
    ReliabilityTest(
      Component(name = "Sequencer", setting = "horizontally scaled"),
      AdverseScenario(
        dependency = "Sequencer database",
        details =
          "all packets send over sequencer-database connection dropped for one second and then the connection is closed until the sequencer reports being inactive",
      ),
      Remediation(
        remediator = "Sequencer",
        action =
          "retry to connect to the database and become active again once connection is restored",
      ),
      outcome = "ping over sequencer is possible whenever he is active",
    )
  ) ignore {
    implicit env => // TODO(#16089): Re-enable this when sequencer HA is working with 3.0 nodes
      import env.*

      // the first sequencer we pick to turn unhealthy is specifically the one participant1 is subscribed to
      // in order make sure we test re-subscription to another sequencer
      val (firstSequencer, otherSequencer) = Seq(
        // participant1 is not connected to sequencer1, that's why it's not included here
        sequencer2,
        sequencer3,
      ).partition(
        _.health.status.trySuccess.connectedParticipants.contains(participant1.id)
      ) match {
        case (Seq(subscribed), Seq(notSubscribed)) => (subscribed, notSubscribed)
        case _ => fail("participant1 should be subscribed to one sequencer")
      }
      val firstSequencerName = firstSequencer.name
      val otherSequencerName = otherSequencer.name
      logger.debug(s"participant is subscribed to $firstSequencerName")

      val proxy = getToxiProxy(firstSequencerName)
      ToxiproxyHelpers.assertNoToxics(
        proxy
      ) // Temporary assertion to double-check this test is running in a clean state

      participant1.health.ping(participant1)

      loggerFactory.suppressWarnings {
        logger.debug(s"ruining $firstSequencerName db connection")
        val toxic =
          proxy.underlying
            .toxics()
            .timeout(s"$firstSequencerName-to-db", ToxicDirection.UPSTREAM, 1000)

        // failover is taking place here, so this ping may take longer
        participant1.health.ping(participant1, timeout = 240.seconds)

        logger.debug(s"waiting for $firstSequencerName to report being inactive")
        waitUntilInactive(firstSequencer)
        otherSequencer.health.status.trySuccess.connectedParticipants should contain(
          participant1.id
        )
        logger.debug(s"$firstSequencerName is inactive, now pinging")

        participant1.health.ping(participant1)

        logger.debug(s"now inactive and restoring $firstSequencerName db access")
        toxic.remove()

        participant1.health.ping(participant1)

        logger.debug(s"waiting for $firstSequencerName to return to being active")
        waitUntilActive(firstSequencer)
      }

      participant1.health.ping(participant1)

      logger.debug(s"shutting down $otherSequencerName")
      // kill the second sequencer so the only sequencer that a participant can use is the first one
      otherSequencer.stop()

      ToxiproxyHelpers.assertNoToxics(
        proxy
      ) // Temporary assertion to double-check this test is running in a clean state
      logger.debug(s"pinging to ensure $firstSequencerName has resumed to being healthy")
      participant1.health.ping(participant1)
      firstSequencer.health.status.trySuccess.connectedParticipants should contain(participant1.id)
  }
}

class RunningSequencerRecoveryOnDatabaseFaultTestPostgres
    extends RunningSequencerRecoveryOnDatabaseFaultTest {
  override def proxyConf(sequencerName: String): SequencerToPostgres =
    SequencerToPostgres(s"$sequencerName-to-postgres", sequencerName)
  override protected val getToxiProxy: String => RunningProxy =
    setupPlugins(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}
