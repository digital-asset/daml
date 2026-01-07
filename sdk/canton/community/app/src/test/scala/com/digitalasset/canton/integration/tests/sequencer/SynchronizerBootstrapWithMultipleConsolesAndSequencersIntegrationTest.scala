// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer

import better.files.*
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.SynchronizerBootstrapWithSeparateConsolesIntegrationTest

import scala.concurrent.duration.DurationInt

trait SynchronizerBootstrapWithMultipleConsolesAndSequencersIntegrationTest
    extends SynchronizerBootstrapWithSeparateConsolesIntegrationTest {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 3,
        numMediators = 2,
      )

  "Nodes in separate consoles after bootstrapping" should {
    "be able to onboard a third sequencer also on a separate console" in { implicit env =>
      import env.*

      for {
        identityFile <- File.temporaryFile("identity", ".proto").map(_.canonicalPath)
        onboardingStateFile <- File
          .temporaryFile("onboarding-state", ".proto")
          .map(_.canonicalPath)
      } yield {

        // user-manual-entry-begin: DynamicallyOnboardSequencerWithSeparateConsoles

        // Third sequencer's console:
        // * write file with identity topology transactions
        {
          sequencer3.topology.transactions.export_identity_transactionsV2(identityFile)
        }

        // Fist and second sequencers' (i.e., owners) console:
        // * load third sequencer's identity transactions
        // * add the third sequencer to the sequencer synchronizer state
        // * write the topology snapshot, sequencer snapshot and static synchronizer parameters to files
        {
          // Store the third sequencer's identity topology transactions on the synchronizer
          sequencer1.topology.transactions
            .import_topology_snapshot_fromV2(identityFile, store = synchronizerId)
          sequencer2.topology.transactions
            .import_topology_snapshot_fromV2(identityFile, store = synchronizerId)

          // wait for the identity transactions to become effective
          sequencer1.topology.synchronisation.await_idle()
          sequencer2.topology.synchronisation.await_idle()

          // find the current sequencer synchronizer state
          val sequencerSynchronizerState =
            sequencer1.topology.sequencers
              .list(store = synchronizerId)
              .headOption
              .getOrElse(sys.error("Did not find sequencer synchronizer state on the synchronizer"))

          // add the third sequencer to the synchronizer state
          val threshold = sequencerSynchronizerState.item.threshold
          val activeSequencers = sequencerSynchronizerState.item.active :+ sequencer3.id
          val newSerial = Some(sequencerSynchronizerState.context.serial.increment)
          sequencer1.topology.sequencers.propose(
            synchronizerId,
            threshold,
            activeSequencers,
            serial = newSerial,
          )
          sequencer2.topology.sequencers.propose(
            synchronizerId,
            threshold,
            activeSequencers,
            serial = newSerial,
          )
          // wait for the topology change to be observed by the sequencer
          utils.retry_until_true(commandTimeouts.bounded) {
            sequencer1.topology.sequencers
              .list(sequencer1.synchronizer_id)
              .headOption
              .map(_.item.allSequencers.forgetNE)
              .getOrElse(Seq.empty)
              .contains(sequencer3.id)
          }

          // fetch the onboarding state and write it to a file
          val onboardingState = sequencer1.setup.onboarding_state_for_sequencer(sequencer3.id)
          utils.write_to_file(onboardingState, onboardingStateFile)
        }

        // Third sequencer's console:
        // * read the onboarding state from file
        // * initialize the third sequencer with the onboarding state
        {
          val onboardingState = utils.read_byte_string_from_file(onboardingStateFile)
          sequencer3.setup.assign_from_onboarding_state(onboardingState)

          sequencer3.health.initialized() shouldBe true
        }

        // user-manual-entry-end: DynamicallyOnboardSequencerWithSeparateConsoles
      }

      // Connect the third participant to the synchronizer via the third sequencer and ping another participant
      participant3.synchronizers.connect_local(sequencer3, daName)
      participant1.health.ping(participant3, timeout = 30.seconds)
    }
  }
}
