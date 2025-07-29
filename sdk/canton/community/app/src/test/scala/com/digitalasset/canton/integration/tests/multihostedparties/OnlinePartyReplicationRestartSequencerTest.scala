// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.sequencing.client.ResilientSequencerSubscription.LostSequencerSubscription
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.*

/** Objective: Ensure OnPR is resilient against sequencer restarts.
  *
  * Setup:
  *   - 2 participants: participant1 hosts a party to replicate to participant2
  *   - 1 sequencer, 1 mediator used for regular canton transaction processing. Sequencer1 used by
  *     party replication and is restarted
  */
sealed trait OnlinePartyReplicationRestartSequencerTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with SharedEnvironment {

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private var alice: PartyId = _
  private var carol: PartyId = _

  private var hasSourceParticipantBeenPaused: Boolean = false
  private var hasSequencerBeenRestarted: Boolean = false

  // Use the test interceptor to block OnPR after replication has started, but before the sequencer
  // hosting the channel is restarted.
  private def createSourceParticipantTestInterceptor() =
    PartyReplicationTestInterceptorImpl.sourceParticipantProceedsIf(stateSP =>
      // Once OnPR has been paused, wait until the sequencer has been restarted.
      if (hasSourceParticipantBeenPaused) hasSequencerBeenRestarted
      else {
        // Otherwise pause once a minimum number of contracts have been sent to the TP
        // to ensure the sequencer restarts happens when both participants are still busy
        // with OnPR.
        if (stateSP.getSentContractsCount.unwrap > 10) {
          hasSourceParticipantBeenPaused = true
          false
        } else true
      }
    )

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(
        (ConfigTransforms.unsafeEnableOnlinePartyReplication(
          Map("participant1" -> (() => createSourceParticipantTestInterceptor()))
        ) :+
          // TODO(#25744): PartyReplicationTargetParticipantProcessor needs to update the in-memory lock state
          //   along with the ActiveContractStore to prevent racy LockableStates internal consistency check failures
          //   such as #26384. Until then, disable the "additional consistency checks".
          ConfigTransforms.disableAdditionalConsistencyChecks)*
      )
      .withSetup { implicit env =>
        import env.*

        // More aggressive AcsCommitmentProcessor checking.
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = PositiveSeconds.tryOfSeconds(1).toConfig),
          )

        participants.all.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonExamplesPath)

        alice = participant1.parties.enable("alice")
        participant2.parties.enable("bob")
        carol = participant1.parties.enable("carol")
      }

  private val numContractsInCreateBatch = 100

  private var serial: PositiveInt = _

  "Create contracts and add party authorization" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val amounts = (1 to numContractsInCreateBatch)
      clue(s"create ${amounts.size * 2} IOUs") {
        IouSyntax.createIous(participant1, alice, carol, amounts)
        IouSyntax.createIous(participant1, carol, alice, amounts)
      }

      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      serial = clue("Alice agrees to have target participant co-host her")(
        participant1.topology.party_to_participant_mappings
          .propose_delta(
            party = alice,
            adds = Seq((targetParticipant, ParticipantPermission.Submission)),
            store = daId,
            serial = None,
            requiresPartyToBeOnboarded = true,
          )
          .transaction
          .serial
      )

      eventually() {
        Seq(sourceParticipant, targetParticipant).foreach(
          _.topology.party_to_participant_mappings
            .list(daId, filterParty = alice.filterString, proposals = true)
            .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
            sourceParticipant.id,
            targetParticipant.id,
          )
        )
      }

  }

  "Resume replicating party after sequencer restart" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val (sourceParticipant, targetParticipant) = (participant1, participant2)
      val addPartyRequestId = clue("Initiate add party async")(
        targetParticipant.parties.add_party_async(
          party = alice,
          synchronizerId = daId,
          sourceParticipant = sourceParticipant,
          serial = serial,
          participantPermission = ParticipantPermission.Submission,
        )
      )

      clue("Wait until OnPR has begun replicating contracts and SP is paused")(eventually() {
        hasSourceParticipantBeenPaused shouldBe true
      })

      loggerFactory.assertLogsUnorderedOptional(
        {
          logger.info("Stopping sequencer")
          sequencer1.stop()
          logger.info("Stopped sequencer")

          // Keep the sequencer down for 5 seconds to have an outage longer than a blip.
          com.digitalasset.canton.concurrent.Threading.sleep(5000)

          logger.info("Restarting sequencer")
          sequencer1.start()
          logger.info("Restarted sequencer")

          hasSequencerBeenRestarted = true

          logger.info("OnPR should eventually be able to finish now that the sequencer is back up")

          // Expect both batches owned by the replicated party.
          val expectedNumContracts = NonNegativeInt.tryCreate(numContractsInCreateBatch * 2)

          // Wait until both SP and TP report that party replication has completed.
          eventuallyOnPRCompletes(
            sourceParticipant,
            targetParticipant,
            addPartyRequestId,
            expectedNumContracts,
          )

          // TODO(#26698): Disconnecting and reconnecting synchronizers seems to be necessary to ensure
          //   submissions don't fail with unknown contract "" errors raised by.
          sourceParticipant.synchronizers.disconnect_all()
          targetParticipant.synchronizers.disconnect_all()
          sourceParticipant.synchronizers.reconnect_all()
          targetParticipant.synchronizers.reconnect_all()

          // Archive the party replication agreement, so that subsequent tests have a clean slate.
          // TODO(#26777): This cleanup should be done by the OnPR process itself.
          val agreement = targetParticipant.ledger_api.javaapi.state.acs
            .await(M.partyreplication.PartyReplicationAgreement.COMPANION)(
              sourceParticipant.adminParty
            )
          targetParticipant.ledger_api.commands
            .submit(
              actAs = Seq(targetParticipant.adminParty),
              commands = agreement.id
                .exerciseDone(targetParticipant.adminParty.toLf)
                .commands
                .asScala
                .toSeq
                .map(LedgerClientUtils.javaCodegenToScalaProto),
              synchronizerId = Some(daId),
            )
            .discard
        },
        // Ignore UNKNOWN status if SP has not found out about the request yet.
        // Besides logging the error produces a CommandFailure error message, hence
        // the "eventually.retryOnTestFailuresOnly = false" above.
        LogEntryOptionality.Optional -> (_.errorMessage should include(
          "UNKNOWN/Add party request id"
        )),
        // Ignore warnings related to regular sequencer/client.
        LogEntryOptionality.OptionalMany -> { e =>
          e.loggerName should include("TimeProofRequestSubmitterImpl")
          e.level shouldBe Level.WARN
        },
        LogEntryOptionality.OptionalMany -> { e =>
          e.loggerName should include("GrpcConnection")
          e.level shouldBe Level.WARN
          e.warningMessage should include("Is the server running?")
        },
        LogEntryOptionality.OptionalMany -> {
          _.shouldBeCantonError(
            LostSequencerSubscription,
            messageAssertion = _ should include("Lost subscription to sequencer"),
            loggerAssertion = _ should include("ResilientSequencerSubscription"),
          )
        },
        // Allow the mediator to optionally have a slow start with the restarted sequencer.
        LogEntryOptionality.Optional -> { e =>
          e.loggerName should include("Mediator")
          e.warningMessage should include regex
            "Detected late processing (or clock skew) of batch with timestamp .* after sequencing"
        },
      )
  }
}

class OnlinePartyReplicationRestartSequencerTestPostgres
    extends OnlinePartyReplicationRestartSequencerTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
