// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.bftsynchronizer

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.data.{ComponentHealthState, ComponentStatus}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.OnboardsNewSequencerNode
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.sequencing.{SequencerConnection, SequencerConnections}
import com.digitalasset.canton.{SequencerAlias, SynchronizerAlias}
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

trait SequencerOnboardingTombstoneTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with OnboardsNewSequencerNode
    with HasCycleUtils {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 1,
        numSequencers = 2,
        numMediators = 1,
      )
      .addConfigTransforms(ConfigTransforms.setExitOnFatalFailures(false))
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq(sequencer1), // sequencer2 will be onboarded in the test
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
          )
        )
      }

  "Basic participants startup, connect to synchronizer via sequencer1, and ping" in {
    implicit env =>
      import env.*

      clue("participant1 connects to sequencer1") {
        participant1.synchronizers.connect_local_bft(
          NonEmpty
            .mk(
              Seq,
              SequencerAlias.tryCreate("seq1x") -> sequencer1,
            )
            .toMap,
          alias = daName,
        )
      }

      participant1.health.ping(participant1.id)
  }

  "Take mediator down and thus produce long-running transaction" in { implicit env =>
    import env.*

    mediator1.stop()
    participant1.dars.upload(CantonExamplesPath)

    // Submit a long-running transaction that mediator1 can only accept or reject after
    // participant1 disconnects from sequencer1.
    val cycle =
      new com.digitalasset.canton.examples.java.cycle.Cycle(
        "My-Cycle",
        participant1.id.adminParty.toProtoPrimitive,
      ).create.commands.asScala.toSeq
    participant1.ledger_api.javaapi.commands.submit_async(
      Seq(participant1.id.adminParty),
      cycle,
      commandId = "long-running-tx-id",
    )

    // Make sure that the participant's request has reached the sequencer
    Threading.sleep(200)
  }

  "Onboard sequencer2 using snapshot from sequencer1" in { implicit env =>
    import env.*

    onboardNewSequencer(
      // synchronizerId,
      initializedSynchronizers(daName).synchronizerId,
      newSequencer = sequencer2,
      existingSequencer = sequencer1,
      synchronizerOwners = initializedSynchronizers(daName).synchronizerOwners,
    )

    // Wait until participant1 has become aware of sequencer2.
    eventually() {
      val sequencerIds = participant1.topology.sequencers
        .list(store = initializedSynchronizers(daName).synchronizerId)
        .flatMap(_.item.active)
      sequencerIds should contain(sequencer2.id)
    }
  }

  "Switch participant1 to sequencer2 and observe synchronizer disconnect due to tombstone" in {
    implicit env =>
      import env.*

      // fetch synchronizer time to ensure participant1's sequencer client is caught up
      // to just before the long-running transaction held up by mediator1 being down before
      // disconnecting from sequencer1.
      participant1.testing.fetch_synchronizer_time(initializedSynchronizers(daName).synchronizerId)
      participant1.synchronizers.disconnect_all()

      // Start the mediator so the long-running transaction's accept or reject can be sequenced.
      mediator1.start()

      // Explicitly modify synchronizer connection to point to sequencer2 instead rather than using connect_local_bft
      // again which would look up the synchronizer connection config under the already registered synchronizerAlias.
      reconfigureParticipantToSequencer(
        daName,
        participant1,
        sequencer2.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq2x")),
      )

      loggerFactory.assertLogsUnorderedOptional(
        {

          clue("participant1 connects to sequencer2 the first time") {
            participant1.synchronizers.reconnect_all(ignoreFailures = false)
          }

          // Eventually we expect participant1's connection to sequencer1 to be severed due to tombstoned
          // accept/reject of the long running transaction.
          eventually() {
            val synchronizers = participant1.synchronizers.list_connected()
            logger.info(s"Connected synchronizers ${synchronizers.mkString(",")}")
            synchronizers shouldBe Seq.empty

            val health = participant1.health.status.trySuccess
            logger.info(s"Health $health")
            health.components should contain(
              ComponentStatus(
                "sequencer-client",
                ComponentHealthState.failed("Disconnected from synchronizer"),
              )
            )
          }
        },
        (
          LogEntryOptionality.OptionalMany,
          (entry: LogEntry) => {
            entry.loggerName should include("SequencerReader$EventsReader")
            entry.warningMessage should include(
              "This sequencer cannot sign the event"
            )
          },
        ),
        (
          LogEntryOptionality.Optional,
          (entry: LogEntry) => {
            entry.loggerName should include("BlockSequencerStateManager")
            entry.warningMessage should include(
              "Terminating subscription due to: This sequencer cannot sign the event"
            )
          },
        ),
        (
          LogEntryOptionality.Required,
          (entry: LogEntry) => {
            entry.loggerName should include("DirectSequencerSubscription")
            entry.warningMessage should include(
              "This sequencer cannot sign the event"
            )
          },
        ),
        (
          LogEntryOptionality.Required,
          (entry: LogEntry) => {
            entry.loggerName should include("ResilientSequencerSubscription")
            entry.warningMessage should (include(
              "Closing resilient sequencer subscription due to error"
            ) and include(
              "FAILED_PRECONDITION/SEQUENCER_TOMBSTONE_ENCOUNTERED"
            ))
          },
        ),
        (
          LogEntryOptionality.Required,
          (entry: LogEntry) => {
            entry.loggerName should include("CantonSyncService")
            entry.errorMessage should (include(
              "SYNC_SERVICE_SYNCHRONIZER_DISCONNECTED"
            ) and include(
              "FAILED_PRECONDITION/SEQUENCER_TOMBSTONE_ENCOUNTERED"
            ))
          },
        ),
      )
  }

  "Switch participant1 back to sequencer1 to consume event tombstoned on sequencer2" in {
    implicit env =>
      import env.*

      reconfigureParticipantToSequencer(
        daName,
        participant1,
        sequencer1.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq1x")),
      )

      clue("participant1 connects to sequencer1") {
        participant1.synchronizers.reconnect_all(ignoreFailures = false)
      }

      participant1.health.ping(participant1.id)
  }

  "Finally switch participant1 back to sequencer2 to consume events past tombstone" in {
    implicit env =>
      import env.*

      participant1.synchronizers.disconnect_all()

      reconfigureParticipantToSequencer(
        daName,
        participant1,
        sequencer2.sequencerConnection.withAlias(SequencerAlias.tryCreate("seq2x")),
      )

      clue("participant1 connects to sequencer2 the second time") {
        participant1.synchronizers.reconnect_all(ignoreFailures = false)
      }

      participant1.health.ping(participant1.id)
  }

  private def reconfigureParticipantToSequencer(
      synchronizerAlias: SynchronizerAlias,
      participant: LocalParticipantReference,
      sequencerConnection: SequencerConnection,
  ): Unit =
    participant.synchronizers.modify(
      synchronizerAlias,
      _.focus(_.sequencerConnections).replace(
        SequencerConnections.single(
          sequencerConnection
        )
      ),
    )

}

class SequencerOnboardingTombstoneTestPostgres extends SequencerOnboardingTombstoneTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
