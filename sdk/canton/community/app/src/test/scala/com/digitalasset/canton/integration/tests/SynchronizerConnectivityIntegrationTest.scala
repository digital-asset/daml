// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.test.evidence.scalatest.OperabilityTestHelpers
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DbConfig, NonNegativeDuration}
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
  UseReferenceBlockSequencerBase,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  SyncServiceInconsistentConnectivity,
  SyncServiceSynchronizerDisabledUs,
  SyncServiceUnknownSynchronizer,
}
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.NotConnectedToAnySynchronizer
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.ConnectionErrors.SynchronizerIsNotAvailable
import com.digitalasset.canton.participant.synchronizer.SynchronizerRegistryError.InitialOnboardingError
import com.digitalasset.canton.sequencing.authentication.MemberAuthentication.MemberAccessDisabled
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.duration.*

sealed trait SynchronizerConnectivityIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OperabilityTestHelpers {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransform(
        ConfigTransforms.updateAllSequencerClientConfigs_(
          _.focus(_.maxConnectionRetryDelay).replace(config.NonNegativeFiniteDuration.ofSeconds(1))
        )
      )
      .addConfigTransform(x =>
        x.focus(_.parameters.timeouts.processing.sequencerInfo)
          .replace(NonNegativeDuration.tryFromDuration(2.seconds))
      )
      .withSetup { env =>
        import env.*
        // Starting conditions of the test:
        // synchronizer1 (daName) - is offline (sequencer1, mediator1 is not running)
        // synchronizer2 (acmeName) - is offline (sequencer2, mediator2 is not running)
        // participant1 - has never been connected to synchronizer1 (daName)
        // participant1 - has been connected to synchronizer2 (acmeName) before
        // participant2 - has never been connected to any synchronizer
        // participant3 - use to check synchronizer registration

        // Bootstrap starts all the nodes (participants are not connected to any synchronizers),
        // below we correct this to arrive to the starting conditions of the test
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.synchronizers.disconnect_local(acmeName)
        mediator1.stop()
        sequencer1.stop()
        mediator2.stop()
        sequencer2.stop()
      }

  private val remedy = operabilityTest("Participant.SyncService")("Sequencer") _

  "A participant connecting for the first time" when_ { setting =>
    "the synchronizer is offline" must_ { cause =>
      remedy(setting)(cause)("Abort a connection attempt and alert") in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect_local(sequencer1, alias = daName),
          _.shouldBeCommandFailure(SyncServiceInconsistentConnectivity),
        )

      }

      "Still reported as not available on a second attempt" in { implicit env =>
        import env.*

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect_local(sequencer1, alias = daName),
          _.shouldBeCommandFailure(SyncServiceInconsistentConnectivity),
        )
      }
    }

    "the synchronizer is back online" must {
      "Succeed subsequently" in { implicit env =>
        import env.*

        sequencer1.start()
        mediator1.start()
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

      "connect_local should be idempotent" in { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.reconnect(daName)
      }

      "disconnect should be idempotent" in { implicit env =>
        import env.*

        participant1.synchronizers.disconnect_local(daName)
        participant1.synchronizers.list_connected() shouldBe empty
        participant1.synchronizers.disconnect_local(daName)
        participant1.synchronizers.disconnect(daName)
      }

      "connect_local should be crash tolerant" in { implicit env =>
        import env.*

        // We simulate a crashed connection attempt by manually storing the DTC into the authorized store
        participant2.topology.synchronizer_trust_certificates.propose(
          participantId = participant2.id,
          synchronizerId = daId,
        )

        participant2.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.list_connected().map(_.synchronizerAlias) shouldBe Seq(daName)
        participant2.synchronizers.disconnect_all()
        participant2.synchronizers.list_connected() shouldBe empty

      }
    }
  }

  "A participant reconnecting to a synchronizer" when_ { setting =>
    "the synchronizer is offline" must_ { cause =>
      remedy(setting)(cause)("Abort if desired upon reconnect") in { implicit env =>
        import env.*

        mediator1.stop()
        sequencer1.stop()
        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.reconnect(daName, retry = false),
          _.shouldBeCommandFailure(SynchronizerIsNotAvailable),
        )
      }

      remedy(setting)(cause)("Notify the operator but keep on connecting") in { implicit env =>
        import env.*
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            participant1.synchronizers.reconnect_local(daName)
            sequencer1.start()
            mediator1.start()
            eventually(timeUntilSuccess = 60.seconds) {
              participant1.health
                .maybe_ping(participant1, timeout = 5.seconds) should not be empty
            }
          },
          forAll(_) { entry =>
            entry.warningMessage should (include(SynchronizerIsNotAvailable.id) or include(
              NotConnectedToAnySynchronizer.id
            ))
          },
        )
        // wait for all ping contracts to be archived
        eventually() {
          participant1.ledger_api.state.acs.of_all() shouldBe empty
        }
        participant1.synchronizers.disconnect_local(daName)
        mediator1.stop()
        sequencer1.stop()
      }

      "Can still be disconnected orderly" in { implicit env =>
        import env.*

        // reconnect should log
        loggerFactory.assertLogs(
          participant1.synchronizers.reconnect(daName),
          _.warningMessage should include(SynchronizerIsNotAvailable.id),
        )

        // now stop reconnection attempts
        participant1.synchronizers.disconnect_local(daName)

        // now reconnect should log again because the disconnect should have disabled any pending reconnect thread
        loggerFactory.assertLogs(
          participant1.synchronizers.reconnect(daName),
          _.warningMessage should include(SynchronizerIsNotAvailable.id),
        )
      }

      "Finally reconnects once the synchronizer is back" in { implicit env =>
        import env.*
        sequencer1.start()
        mediator1.start()
        loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          eventually(timeUntilSuccess = 30.seconds) {
            participant1.health
              .maybe_ping(participant1, timeout = 5.seconds) should not be empty
          },
          forAll(_) { entry =>
            entry.shouldBeCantonErrorCode(NotConnectedToAnySynchronizer)
          },
        )
      }
    }

    "A participant cannot reconnect to an unknown synchronizer alias" in { implicit env =>
      import env.*

      assertThrowsAndLogsCommandFailures(
        participant1.synchronizers.reconnect("unknown-synchronizer-alias", retry = false),
        _.shouldBeCommandFailure(SyncServiceUnknownSynchronizer),
      )

      participant1.synchronizers.disconnect(daName)
    }
  }

  "A participant reconnecting to two synchronizers" when_ { setting =>
    "one synchronizer is offline" must_ { cause =>
      remedy(setting)(cause)("Abort both if desired upon reconnect") in { implicit env =>
        import env.*

        participant1.synchronizers.list_connected() shouldBe empty

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.reconnect_all(ignoreFailures = false),
          // error is logged on participant and console
          _.commandFailureMessage should (include(SynchronizerIsNotAvailable.id) and include(
            acmeName.unwrap
          )),
        )

        participant1.synchronizers.list_connected() shouldBe empty

      }

      remedy(setting)(cause)("Succeed on one if requested upon reconnect") in { implicit env =>
        import env.*

        participant1.synchronizers.list_connected() shouldBe empty

        loggerFactory.assertLogs(
          participant1.synchronizers.reconnect_all(ignoreFailures = true),
          _.warningMessage should (include(SynchronizerIsNotAvailable.id) and include(
            acmeName.unwrap
          )),
        )

        participant1.synchronizers.list_connected().map(_.synchronizerAlias) shouldBe Seq(daName)

        // now start acme
        sequencer2.start()
        mediator2.start()

        eventually() {
          participant1.synchronizers.list_connected().map(_.synchronizerAlias).toSet shouldBe Set(
            daName,
            acmeName,
          )
        }

        participant1.health.ping(participant1)
      }

      "Reconnects are idempotent" in { implicit env =>
        import env.*

        participant1.synchronizers.reconnect_all()
        participant1.synchronizers.reconnect_all()
        participant1.synchronizers.reconnect(daName)
        participant1.synchronizers.reconnect(daName)

      }
    }
  }

  "A participant reconnecting to a synchronizer" when_ { setting =>
    "the participant has revoked its synchronizer trust certificate" must_ { cause =>
      remedy(setting)(cause)("Fail the connection attempt") in { implicit env =>
        import env.*

        participant1.synchronizers.connect_local(sequencer1, alias = daName)

        // revoke the synchronizer trust certificate of participant1
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            participant1.topology.synchronizer_trust_certificates.propose(
              participantId = participant1.id,
              synchronizerId = daId,
              change = TopologyChangeOp.Remove,
            )
            eventually() {
              participant1.synchronizers.is_connected(daId) shouldBe false
              sequencer1.topology.participant_synchronizer_states
                .active(daId, participant1.id) shouldBe false
            }
          },
          forEvery(_) { entry =>
            entry.message should (include(
              MemberAccessDisabled(participant1.id).reason
            ) or include(
              SyncServiceSynchronizerDisabledUs.id
            ) or // The participant might have started to refresh the token before being disabled, but the refresh request
              // is processed by the sequencer after the participant is disabled
              include(
                "Health-check service responded NOT_SERVING for"
              ) or include("Token refresh aborted due to shutdown")
              // the participant might not actually get the dispatched transaction delivered,
              // because the sequencer may cut the participant's connection before delivering the topology broadcast
              or include regex ("Waiting for transaction .* to be observed"))
          },
        )

        assertThrowsAndLogsCommandFailures(
          participant1.synchronizers.connect_local(sequencer1, alias = daName),
          entry => {
            entry.shouldBeCommandFailure(InitialOnboardingError)
            entry.commandFailureMessage should include(
              s"${participant1.id} has previously been off-boarded and cannot onboard again"
            )
          },
        )
        sequencer1.topology.synchronisation.await_idle()
        // check that the sequencer did not accept the participant's synchronizer trust certificate
        sequencer1.topology.participant_synchronizer_states
          .active(daId, participant1.id) shouldBe false
      }
    }
  }

  "A participant registering a synchronizer" should {
    "take handshake flag into account" in { implicit env =>
      import env.*

      nodes.local.start()

      def testWithoutHandshake() = {
        participant3.synchronizers.register(sequencer1, daName, performHandshake = false)
        participant3.synchronizers.is_registered(daName) shouldBe true
        participant3.synchronizers.is_connected(daName) shouldBe false

        participant3.underlying.value.sync.syncPersistentStateManager
          .get(daId)
          .isDefined shouldBe false
      }

      def testWithHandshake() = {
        participant3.synchronizers.register(sequencer2, acmeName, performHandshake = true)
        participant3.synchronizers.is_registered(acmeName) shouldBe true
        participant3.synchronizers.is_connected(acmeName) shouldBe false

        participant3.underlying.value.sync.syncPersistentStateManager
          .get(acmeId)
          .isDefined shouldBe true
      }

      // No handshake
      testWithoutHandshake()
      testWithoutHandshake() // idempotency
      participant3.synchronizers.connect_local(
        sequencer1,
        daName,
      ) // we can connect after registration
      participant3.underlying.value.sync.syncPersistentStateManager
        .get(daId)
        .isDefined shouldBe true

      // Handshake
      testWithHandshake()
      testWithHandshake() // idempotency
      participant3.synchronizers.connect(sequencer2, acmeName) // we can connect after registration
      participant3.synchronizers.is_connected(acmeName) shouldBe true
    }
  }
}

//class SynchronizerConnectivityIntegrationTestH2 extends SynchronizerConnectivityIntegrationTest {
//  registerPlugin(new UseH2(loggerFactory))
//}

class SynchronizerConnectivityReferenceIntegrationTestPostgres
    extends SynchronizerConnectivityIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = UseReferenceBlockSequencerBase.MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}

// TODO(#16761): Disabled because the BFT sequencer is not crash tolerant yet
abstract class SynchronizerConnectivityBftOrderingIntegrationTestPostgres
    extends SynchronizerConnectivityIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseBftSequencer(
      loggerFactory,
      sequencerGroups = UseReferenceBlockSequencerBase.MultiSynchronizer(
        Seq(Set(InstanceName.tryCreate("sequencer1")), Set(InstanceName.tryCreate("sequencer2")))
      ),
    )
  )
}
