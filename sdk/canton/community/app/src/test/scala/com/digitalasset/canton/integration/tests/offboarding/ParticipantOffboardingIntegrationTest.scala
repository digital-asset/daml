// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.offboarding

import com.digitalasset.canton.admin.api.client.data.{
  StaticSynchronizerParameters,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Remove
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt

class ParticipantOffboardingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Manual

  private var synchronizerId: SynchronizerId = _
  private var staticParameters: StaticSynchronizerParameters = _
  private var synchronizerOwners: Seq[InstanceReference] = _

  "A domain with multiple participants" should {

    "Basic synchronizer startup" in { implicit env =>
      import env.*

      clue("starting up participants") {
        // for now we need a participant to effect changes to the synchronizer after the initial bootstrap
        participant1.start()
        participant2.start()
        participant3.start()
      }
      clue("start sequencer") {
        sequencers.local.start()
      }
      clue("start mediator") {
        mediators.local.start()
      }

      staticParameters =
        StaticSynchronizerParameters.defaults(sequencer1.config.crypto, testedProtocolVersion)

      synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1)
      synchronizerId = clue("bootstrapping the synchronizer") {
        bootstrap.synchronizer(
          "test-synchronizer",
          sequencers = sequencers.local,
          // Bootstrapping the synchronizer with the mediator only connected to sequencer1
          //  because changing a mediator's connection is currently unsupported and
          //  the goal of this test is not to check sequencer connection fail-over.
          mediatorsToSequencers = Map(
            mediator1 -> (Seq(sequencer1), PositiveInt.one, NonNegativeInt.zero)
          ),
          synchronizerOwners = synchronizerOwners,
          synchronizerThreshold = PositiveInt.two,
          staticParameters,
          mediatorRequestAmplification = SubmissionRequestAmplification.NoAmplification,
          mediatorThreshold = PositiveInt.one,
        )
      }
    }

    "Onboard participants to sequencer and send a ping" in { implicit env =>
      import env.*

      clue("participants connect to sequencers") {
        participant1.synchronizers.connect_local(sequencer1, daName)
        participant2.synchronizers.connect_local(sequencer1, daName)
        participant3.synchronizers.connect_local(sequencer1, daName)
      }

      clue("participants can ping each other") {
        participant1.health.ping(participant2.id)
        participant1.health.ping(participant3.id)
        participant2.health.ping(participant3.id)
      }
    }

    "successfully off-board a participant" in { implicit env =>
      import env.*

      clue("offboard participant2") {
        // user-manual-entry-begin: OffboardParticipant
        // Unauthorize the participant on the synchronizer by removing its permissions
        synchronizerOwners.foreach { synchronizerOwner =>
          synchronizerOwner.topology.participant_synchronizer_permissions
            .list(synchronizerId, filterUid = participant2.filterString)
            .map(_.item.permission)
            .foreach(permission =>
              synchronizerOwner.topology.participant_synchronizer_permissions
                .propose(synchronizerId, participant2.id, permission = permission, change = Remove)
            )
        }

        eventually() {
          forAll(synchronizerOwners) { synchronizerOwner =>
            synchronizerOwner.topology.participant_synchronizer_permissions
              .list(synchronizerId, filterUid = participant2.filterString) shouldBe empty
          }
        }

        // Disable the participant on all the sequencers to remove any sequencer data associated with it
        //  and allow sequencer pruning
        sequencers.all.foreach(_.repair.disable_member(participant2))
        // user-manual-entry-end: OffboardParticipant
      }

      clue("check that pings that don't involve the off-boarded participant still work") {
        participant1.health.ping(participant3, timeout = 30.seconds)
      }

      clue("check that pings that involve the off-boarded participant don't work") {
        forAll(Seq(participant1, participant3)) { sourceParticipant =>
          loggerFactory.assertLogsSeqString(
            SuppressionRule.LevelAndAbove(Level.WARN),
            Seq("responder did not respond in time", "is disabled at the sequencer"),
          ) {
            a[CommandFailure] should be thrownBy sourceParticipant.health.ping(
              participant2,
              timeout = 1.second,
            )
          }
        }
      }
    }
  }
}
