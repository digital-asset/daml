// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration
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

/** Objective: Ensure OnPR is resilient against sequencer restarts and SP-synchronizer reconnects.
  *
  * Setup:
  *   - 2 participants: participant1 hosts a party to replicate to participant2
  *   - 1 sequencer, 1 mediator used for regular canton transaction processing. Sequencer1 used by
  *     party replication and is restarted
  */
sealed trait OnlinePartyReplicationRecoverFromDisruptionsTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with SharedEnvironment {

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private var alice: PartyId = _
  private var carol: PartyId = _
  private var dora: PartyId = _
  private var emily: PartyId = _

  private var hasSourceParticipantBeenPaused: Boolean = false
  private var hasDisruptionBeenFixed: Boolean = false

  // Use the test interceptor to block OnPR after replication has started, but before the sequencer
  // hosting the channel is restarted.
  private def createSourceParticipantTestInterceptor() =
    PartyReplicationTestInterceptorImpl.sourceParticipantProceedsIf(stateSP =>
      // Once OnPR has been paused, wait until the disruption has been lifted unblocking OnPR.
      if (hasSourceParticipantBeenPaused) hasDisruptionBeenFixed
      else {
        // Otherwise pause once a minimum number of contracts have been sent to the TP
        // to ensure the disruption happens when both participants are still busy
        // with OnPR.
        if (stateSP.sentContractsCount.unwrap > 10) {
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
        dora = participant1.parties.enable("dora")
        emily = participant1.parties.enable("emily")
      }

  private val numContractsInCreateBatch = PositiveInt.tryCreate(100)

  private def initiateOnPRAndResetTestInterceptor(
      partyToReplicate: PartyId,
      fellowContractStakeholder: PartyId,
  )(implicit env: integration.TestConsoleEnvironment): (String, PositiveInt) = {
    import env.*

    // Expect both batches owned by the replicated party.
    val expectedNumContracts = numContractsInCreateBatch * 2
    val amounts = (1 to numContractsInCreateBatch.unwrap)
    clue(s"create ${expectedNumContracts.unwrap} IOUs") {
      IouSyntax.createIous(participant1, partyToReplicate, fellowContractStakeholder, amounts)
      IouSyntax.createIous(participant1, fellowContractStakeholder, partyToReplicate, amounts)
    }

    val (sourceParticipant, targetParticipant) = (participant1, participant2)

    val serial = clue(s"$partyToReplicate agrees to have target participant co-host her")(
      participant1.topology.party_to_participant_mappings
        .propose_delta(
          party = partyToReplicate,
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
          .list(daId, filterParty = partyToReplicate.filterString, proposals = true)
          .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      )
    }

    // Reset the test interceptor for the next test.
    hasSourceParticipantBeenPaused = false
    hasDisruptionBeenFixed = false

    val requestId = clue(s"Initiate add party async for $partyToReplicate")(
      targetParticipant.parties.add_party_async(
        party = partyToReplicate,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = serial,
        participantPermission = ParticipantPermission.Submission,
      )
    )

    clue("Wait until OnPR has begun replicating contracts and SP is paused")(eventually() {
      hasSourceParticipantBeenPaused shouldBe true
    })

    (requestId, expectedNumContracts)
  }

  // Wait for 5 seconds to ensure the disruption produces a noticed "outage" longer than a blip.
  private def sleepLongEnoughForDisruptionToBeNoticed(): Unit = Threading.sleep(5000)

  "Finish replicating party after sequencer restart" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val (addPartyRequestId, expectedNumContracts) =
        initiateOnPRAndResetTestInterceptor(alice, carol)
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      loggerFactory.assertLogsUnorderedOptional(
        {
          clue("Stop sequencer")(
            sequencer1.stop()
          )

          sleepLongEnoughForDisruptionToBeNoticed()

          clue("Restart sequencer")(
            sequencer1.start()
          )
          hasDisruptionBeenFixed = true

          logger.info("OnPR should eventually be able to finish now that the sequencer is back up")

          // Wait until both SP and TP report that party replication has completed.
          eventuallyOnPRCompletes(
            sourceParticipant,
            targetParticipant,
            addPartyRequestId,
            expectedNumContracts.toNonNegative,
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
        LogEntryOptionality.OptionalMany -> (_.errorMessage should include(
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

  "Finish replicating party after source participant synchronizer reconnect" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*

      val (addPartyRequestId, expectedNumContracts) =
        initiateOnPRAndResetTestInterceptor(dora, emily)
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      loggerFactory.assertLogsUnorderedOptional(
        {
          clue("Disconnect source participant from synchronizer")(
            sourceParticipant.synchronizers.disconnect(daName)
          )

          sleepLongEnoughForDisruptionToBeNoticed()

          clue("Reconnect source participant to synchronizer")(
            sourceParticipant.synchronizers.reconnect_local(daName)
          )

          hasDisruptionBeenFixed = true

          // Wait until both SP and TP report that party replication has completed.
          eventuallyOnPRCompletes(
            sourceParticipant,
            targetParticipant,
            addPartyRequestId,
            expectedNumContracts.toNonNegative,
          )

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
        LogEntryOptionality.OptionalMany -> (_.errorMessage should include(
          "UNKNOWN/Add party request id"
        )),
        // On the sequencer-channel-service side, expect warnings about the SP cancelling and forwarding the error to the TP.
        LogEntryOptionality.Required -> { entry =>
          entry.loggerName should include("GrpcSequencerChannelMemberMessageHandler")
          entry.warningMessage should include regex "Member message handler received error CANCELLED: client cancelled. Forwarding error to recipient"
        },
        LogEntryOptionality.Required -> { entry =>
          entry.loggerName should include("GrpcSequencerChannelMemberMessageHandler")
          entry.warningMessage should include regex "Request stream error CANCELLED: client cancelled has terminated connection"
        },
      )
  }
}

class OnlinePartyReplicationRecoverFromDisruptionsTestPostgres
    extends OnlinePartyReplicationRecoverFromDisruptionsTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
