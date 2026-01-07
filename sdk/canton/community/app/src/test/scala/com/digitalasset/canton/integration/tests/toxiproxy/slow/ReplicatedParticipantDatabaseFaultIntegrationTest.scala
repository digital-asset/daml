// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.daml.test.evidence.scalatest.AccessTestScenario
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.daml.test.evidence.tag.Reliability.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DbLockedConnectionConfig
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres, UseSharedStorage}
import com.digitalasset.canton.integration.tests.*
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  EnvironmentSetupPlugin,
  SharedEnvironment,
}
import eu.rekawek.toxiproxy.model.ToxicDirection

import scala.concurrent.duration.*

trait ReplicatedParticipantDatabaseFaultIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with ReplicatedParticipantTestSetup
    with ReliabilityTestSuite {

  def proxyConf: ProxyConfig

  lazy val toxiProxyConf = UseToxiproxy.ToxiproxyConfig(List(proxyConf))
  lazy val toxiproxyPlugin = new UseToxiproxy(toxiProxyConf)

  protected val getToxiProxy: () => RunningProxy

  protected def sharedStorageToxiproxy: Boolean = false

  protected def setupPluginsWithToxiproxy(
      storagePlugin: EnvironmentSetupPlugin,
      blockSequencerPlugin: UseBftSequencer,
  ): () => RunningProxy = {
    registerPlugin(storagePlugin)
    registerPlugin(blockSequencerPlugin)

    val sharedStoragePlugin =
      UseSharedStorage.forParticipants(
        activeParticipantName,
        Seq(passiveParticipantName),
        loggerFactory,
      )

    if (sharedStorageToxiproxy) {
      registerPlugin(toxiproxyPlugin)
      registerPlugin(sharedStoragePlugin)
    } else {
      registerPlugin(sharedStoragePlugin)
      // Configure toxiproxy only after the shared storage has been configured
      registerPlugin(toxiproxyPlugin)
    }

    // Return a function that returns the running proxy during tests
    () => toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withManualStart
      .withSetup(setupReplicas(_))
      .withTeardown { _ =>
        ToxiproxyHelpers.removeAllProxies(
          toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient
        )
      }

}

trait ActiveParticipantDatabaseFaultIntegrationTest
    extends ReplicatedParticipantDatabaseFaultIntegrationTest
    with AccessTestScenario {

  "A replicated participant" must {

    // TODO(test-coverage): Test with different forms of connection problems (closing, reject, no bandwith/timeout)

    "fail-over on a database connection drop of the active replica".taggedAs_(outcome =>
      ReliabilityTest(
        Component(name = "Participant", setting = "active replica"),
        AdverseScenario(
          dependency = "database",
          details = "after all data is dropped for one second, the TCP connection is closed orderly",
        ),
        Remediation(
          remediator = "participant",
          action = "become passive and retry to connect to the database",
        ),
        outcome = outcome,
      )
    ) in { implicit env =>
      import env.*

      // P1 is active and can ping, p2 is passive
      activePing(participant1, participant3)
      failPassivePing(participant2, participant3)

      // Drop the connection from the active participant (= p1) to the database to trigger a fail-over
      logger.debug(s"Cutting DB connection for $activeParticipantName")

      loggerFactory.suppressWarnings {
        // Slow down and terminate the connection
        val toxic = getToxiProxy().underlying
          .toxics()
          // Block traffic for 5 minutes before cutting the connection
          // The active participant should still transition to passive in less time
          .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 5.minutes.toMillis)

        // Wait until p1 is passive and p2 active
        waitActive(participant2)
        waitPassive(participant1)

        // Re-enable the connection, p1 should now still be passive
        logger.debug(s"Re-enabling DB connection for $activeParticipantName")
        toxic.remove()
      }

      // P2 is active and P1 passive
      activePing(participant2, participant3)
      failPassivePing(participant1, participant3, afterFailure = true)
    }
  }

  "An active participant replica" should {

    // The active participant replica should remain active, but there's no guarantee as a passive replica can steal the
    // lock despite the interruption being short.
    "remain active on short connection interruption unless lock is stolen by passive replica"
      .taggedAs(
        ReliabilityTest(
          Component(name = "Participant", setting = "active replica"),
          AdverseScenario(
            dependency = "database",
            details =
              "all packets over the connection are dropped for one second and then the TCP connection " +
                "is orderly closed for (`healthCheckPeriod` + 1) seconds",
          ),
          Remediation(
            remediator = "participant",
            action = "retry to establish database connection to remain active",
          ),
          outcome = "participant remains active unless lock is stolen by passive replica",
        )
      ) in { implicit env =>
      import env.*

      // Note: this is SharedEnvironment, which is why P2 is now the active participant after P1 and P2
      // got swapped in the test above, switch back
      participant2.replication.set_passive()

      activePing(participant1, participant3)
      failPassivePing(participant2, participant3)

      loggerFactory.suppressWarnings {
        logger.debug(s"Cutting DB connection for $participant1")

        // Slow down and terminate the connection
        val toxic = getToxiProxy().underlying
          .toxics()
          .timeout("db-conn-timeout", ToxicDirection.UPSTREAM, 1000)

        val waitTime = DbLockedConnectionConfig().healthCheckPeriod.unwrap + 1.second
        Threading.sleep(waitTime.toMillis)

        logger.debug(s"Enabling DB connection for $participant1")

        // Enable the connection to the database again
        toxic.remove()
      }

      // Eventually either p1 or p2 must be active
      val activeParticipant = eventually(timeout) {
        val isActiveP1 = isActive(participant1)
        val isActiveP2 = isActive(participant2)

        val activeParticipant =
          if (isActiveP1)
            participant1
          else if (isActiveP2)
            participant2
          else
            fail("neither p1 nor p2 was active")

        // Ensure the participant is actually active
        activePing(activeParticipant, participant3)

        activeParticipant
      }

      if (activeParticipant == participant1) {
        failPassivePing(participant2, participant3)
      } else {
        failPassivePing(participant1, participant3, afterFailure = true)
      }

    }
  }
}

trait PassiveParticipantDatabaseFaultIntegrationTest
    extends ReplicatedParticipantDatabaseFaultIntegrationTest {

  "A passive participant replica" must {

    "remain passive on database connection drop".taggedAs(
      ReliabilityTest(
        Component(name = "Participant", setting = "passive replica"),
        AdverseScenario(
          dependency = "database",
          details =
            "after all data is dropped for three seconds, the TCP connection is closed orderly",
        ),
        Remediation(
          remediator = "participant",
          action = "retry to connect to the database and remain passive",
        ),
        outcome = "participant is passively available after connection is reestablished",
      )
    ) in { implicit env =>
      import env.*

      // P1 is active and can ping
      activePing(participant1, participant3)
      failPassivePing(participant2, participant3)

      loggerFactory.suppressWarnings {
        // Drop the connection from the passive participant (= p2) to the database, p2 should remain passive
        logger.debug(s"Cutting DB connection for $passiveParticipantName")
        val toxic = getToxiProxy().underlying
          .toxics()
          .timeout("db-conn-timeout", ToxicDirection.DOWNSTREAM, 3000)

        assert(isActive(participant1))
        assert(!isActive(participant2))

        // Re-enable the connection, p1 should now still be passive
        toxic.remove()
      }

      failPassivePing(participant2, participant3, afterFailure = true)
      activePing(participant1, participant3)
    }

  }

}

class ActiveParticipantDatabaseFaultIntegrationTestPostgres
    extends ActiveParticipantDatabaseFaultIntegrationTest {

  // Configure toxiproxy for the active replica (=p1)
  override def proxyConf: ProxyConfig =
    ParticipantToPostgres(s"p1-to-postgres", activeParticipantName)

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}

class PassiveParticipantDatabaseFaultIntegrationTestPostgres
    extends PassiveParticipantDatabaseFaultIntegrationTest {

  // Configure toxiproxy for the passive replica (=p2)
  override def proxyConf: ProxyConfig =
    ParticipantToPostgres(s"p2-to-postgres", passiveParticipantName)

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )

}

trait AllReplicatedParticipantDatabaseFaultIntegrationTest
    extends ReplicatedParticipantDatabaseFaultIntegrationTest {

  // TODO(#15920): due to a race in the reconnection logic, recovering the DB connection
  //  and transitioning to active takes a long time
  override protected lazy val timeout: FiniteDuration = 3.minutes

  override def sharedStorageToxiproxy: Boolean = true

  "A replicated participant" must {

    "fail-over on a database connection drop on all replicas".taggedAs(
      ReliabilityTest(
        Component(name = "Participant", setting = "all replicas"),
        AdverseScenario(
          dependency = "database",
          details =
            "after all data is dropped for two seconds, the TCP connection is closed orderly",
        ),
        Remediation(
          remediator = "participant",
          action = "become passive and retry to connect to the database",
        ),
        outcome = "one participant becomes active again once DB connection is restored",
      )
    ) in { implicit env =>
      import env.*

      // P1 is active and can ping, p2 is passive
      activePing(participant1, participant3)
      failPassivePing(participant2, participant3)

      logger.debug(s"Cutting DB connection for all replicas")

      val active = loggerFactory.suppressWarnings {
        // Slow down and terminate the connection
        val toxic = getToxiProxy().underlying
          .toxics()
          .timeout("db-con-timeout", ToxicDirection.UPSTREAM, 2000)

        // Wait until all replicas are passive
        clue("Waiting for P1 to become passive")(waitPassive(participant1))
        clue("Waiting for P2 to become passive")(waitPassive(participant2))

        // Re-enable the connection, p1 should now still be passive
        toxic.remove()

        clue("Waiting for P1 or P2 to become active")(
          waitUntilOneActive(participant1, participant2)
        )
      }

      activePing(active, participant3)
    }
  }
}

class AllParticipantDatabaseFaultIntegrationTestPostgres
    extends AllReplicatedParticipantDatabaseFaultIntegrationTest {

  override def proxyConf: ProxyConfig =
    ParticipantToPostgres(s"p1-to-postgres", activeParticipantName)

  override protected val getToxiProxy: () => RunningProxy =
    setupPluginsWithToxiproxy(
      new UsePostgres(loggerFactory),
      new UseBftSequencer(loggerFactory),
    )
}
