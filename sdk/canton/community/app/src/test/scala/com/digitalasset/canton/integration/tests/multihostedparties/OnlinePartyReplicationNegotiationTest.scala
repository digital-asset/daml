// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction.TreeEvent.Kind.{Created, Exercised}
import com.daml.ledger.api.v2.transaction.{TransactionTree, TreeEvent}
import com.daml.ledger.api.v2.value.Value.Sum.Party
import com.daml.ledger.api.v2.value.{RecordField, Value}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionTreeWrapper
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalParticipantReference,
}
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.sequencing.{
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.*

/** Objective: Test the negotiation of party replication via the PartyReplication.daml workflow.
  *
  * Setup:
  *   - 3 participants: the first two host the party to replicate and the third one doesn't for
  *     negative testing
  *   - 4 sequencers: sequencer2 and sequencer3 are connected to both participants 1 and 2 (see
  *     connectivityMap), and only sequencer1, sequencer2, and sequencer4 support channels (see
  *     selectivelyEnablePartyReplicationOnSequencers).
  */
sealed trait OnlinePartyReplicationNegotiationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  private def connectivityMap(implicit env: TestConsoleEnvironment) = {
    // Connect participants 1 and 2 to different sequencers to test negotiating sequencer
    import env.*
    Map(
      participant1 -> Seq(sequencer1, sequencer2, sequencer3),
      participant2 -> Seq(sequencer2, sequencer3, sequencer4),
      participant3 -> Seq(sequencer1, sequencer2, sequencer3),
    )
  }

  private def selectivelyEnablePartyReplicationOnSequencers(
      sequencer: String,
      config: SequencerNodeConfig,
  ): SequencerNodeConfig =
    // Enable channels on all sequencer except sequencer3 to test
    // that a usable sequencer is chosen.
    config
      .focus(_.parameters.unsafeEnableOnlinePartyReplication)
      .replace(sequencer != "sequencer3")

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val aliceName = "Alice"

  private var alice: PartyId = _

  private val channelServiceNotImplementedWarning =
    "GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: .*SequencerChannelService/Ping"

  private val validOnPRIdS = Hash
    .build(HashPurpose.OnlinePartyReplicationId, HashAlgorithm.Sha256)
    .finish()
    .toHexString

  private val unspecifiedTopologySerial = 0L

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 3,
        numSequencers = 4,
        numMediators = 1,
      )
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.unsafeOnlinePartyReplication)
            .replace(Some(UnsafeOnlinePartyReplicationConfig()))
        ),
        ConfigTransforms.updateAllSequencerConfigs(selectivelyEnablePartyReplicationOnSequencers),
      )
      .withNetworkBootstrap { implicit env =>
        import env.*
        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](mediator1),
            sequencers = Seq(sequencer1, sequencer2, sequencer3, sequencer4),
            mediators = Seq(mediator1),
            synchronizerThreshold = PositiveInt.one,
          )
        )
      }
      .withSetup { implicit env =>
        import env.*
        connectivityMap.foreach { case (participant, sequencers) =>
          val sequencerConnections = SequencerConnections.tryMany(
            sequencers
              .map(s => s.sequencerConnection.withAlias(SequencerAlias.tryCreate(s.name))),
            // A threshold of 2 ensures that each participant connects to all the three sequencers in the connectivity map
            // TODO(#19911) Make this properly configurable
            sequencerTrustThreshold = PositiveInt.two,
            submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
          )
          participant.synchronizers.connect_by_config(
            SynchronizerConnectionConfig(
              synchronizerAlias = daName,
              sequencerConnections = sequencerConnections,
              manualConnect = false,
              synchronizerId = None,
              priority = 0,
              initialRetryDelay = None,
              maxRetryDelay = None,
              SynchronizerTimeTrackerConfig(),
            ),
            validation = SequencerConnectionValidation.Active,
            synchronize = Some(env.commandTimeouts.bounded),
          )
        }

        alice = participant1.parties.enable(
          aliceName,
          synchronizeParticipants = Seq(participant2),
        )
      }

  "Obtain agreement to establish to replicate a party" in { implicit env =>
    import env.*
    val (sourceParticipant, targetParticipant) = (participant1, participant2)

    loggerFactory.assertLogs(
      {
        clue("Initiate add party async")(
          targetParticipant.parties.add_party_async(
            party = alice,
            synchronizerId = daId,
            sourceParticipant = Some(sourceParticipant),
            serial = None,
          )
        )

        def partyToReplicate(create: CreatedEvent) = create.createArguments
          .getOrElse(fail("missing arguments record"))
          .fields
          .collect { case RecordField("partyId", Some(Value(Party(partyId)))) =>
            partyId
          }
          .loneElement

        Seq(sourceParticipant, targetParticipant).foreach { participant =>
          clue(s"Checking participant ${participant.name}: ") {
            eventually() {
              val trees = participant.ledger_api.updates.trees(
                Set(sourceParticipant.adminParty),
                completeAfter = 10,
                timeout = config.NonNegativeDuration.ofSeconds(1),
              )

              // The TP asks the SP to replicate Alice via proposal
              val createProposal = trees
                .collect { case TransactionTreeWrapper(tree: TransactionTree) =>
                  tree.eventsById.values.collect {
                    case TreeEvent(Created(event))
                        if event.templateId.contains(
                          PartyReplicationAdminWorkflow.proposalTemplate
                        ) =>
                      event
                  }
                }
                .flatten
                .loneElement
              val party = partyToReplicate(createProposal)
              createProposal.signatories shouldBe Seq(
                targetParticipant.adminParty.toProtoPrimitive
              )
              party shouldBe alice.toProtoPrimitive

              // The SP accepts the party replication proposal
              val accept = trees
                .collect { case TransactionTreeWrapper(tree: TransactionTree) =>
                  tree.eventsById.values.collect {
                    case TreeEvent(Exercised(event))
                        if event.templateId.contains(
                          PartyReplicationAdminWorkflow.proposalTemplate
                        ) =>
                      event
                  }
                }
                .flatten
                .loneElement
              accept.choice shouldBe "Accept"
              accept.consuming shouldBe true
              accept.contractId shouldBe createProposal.contractId
              accept.actingParties shouldBe Seq(
                sourceParticipant.adminParty.toProtoPrimitive
              )

              // There is an active party replication agreement contract signed by P1 and P2
              val agreement = participant.ledger_api.javaapi.state.acs
                .await(M.partyreplication.PartyReplicationAgreement.COMPANION)(
                  sourceParticipant.adminParty
                )
              agreement.signatories.asScala.toSet shouldBe Set(
                sourceParticipant.adminParty.toProtoPrimitive,
                targetParticipant.adminParty.toProtoPrimitive,
              )
              agreement.data.partyId shouldBe alice.toProtoPrimitive
              // Only sequencer2 remains for the party replication as not both participants are
              // connected to sequencers 1 and 4, and sequencer3 has not enabled sequencer channels.
              agreement.data.sequencerUid shouldBe sequencer2.id.uid.toProtoPrimitive
            }
          }
        }

        // Archive the party replication agreement, so that subsequent tests have a clean slate.
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
      _.warningMessage should include regex channelServiceNotImplementedWarning,
    )

    // Wait until both participants observe that both participants are allowed to host the party.
    eventually()(Seq(sourceParticipant, targetParticipant).foreach { participant =>
      val p2p = participant.topology.party_to_participant_mappings
        .list(daId, filterParty = alice.filterString)
        .loneElement
        .item

      p2p.participants.map(_.participantId) should contain theSameElementsAs Seq(
        sourceParticipant.id,
        targetParticipant.id,
      )
    })
  }

  "Prevent malformed party replication proposals" in { implicit env =>
    import env.*
    val participantWithParty = participant1
    val participantWithParty2 = participant2
    val participantWithoutParty = participant3

    def testProposalError(
        log: String,
        errorRegex: String,
        targetParticipant: LocalParticipantReference = participantWithoutParty,
        sourceParticipantO: Option[LocalParticipantReference] = Some(participantWithParty2),
        synchronizerId: SynchronizerId = daId,
        serialO: Option[PositiveInt] = None,
    ): Unit =
      clue(log)(
        loggerFactory.assertThrowsAndLogsUnorderedOptional[CommandFailure](
          targetParticipant.parties.add_party_async(
            party = alice,
            synchronizerId = synchronizerId,
            sourceParticipant = sourceParticipantO.map(_.id),
            serial = serialO,
          ),
          LogEntryOptionality.Required -> (_.errorMessage should include regex errorRegex),
          LogEntryOptionality.Optional -> (_.warningMessage should include regex channelServiceNotImplementedWarning),
        )
      )

    testProposalError(
      "source-participant-does-not-host-party",
      "Party .* is not hosted by source participant",
      sourceParticipantO = Some(participantWithoutParty),
      targetParticipant = participantWithParty,
    )

    testProposalError(
      "target-participant-already-hosts-party",
      "Party .* is already hosted by target participant",
      targetParticipant = participantWithParty,
    )

    testProposalError(
      "matching-participants",
      "Source and target participants .* cannot match",
      sourceParticipantO = Some(participantWithParty),
      targetParticipant = participantWithParty,
    )

    testProposalError(
      "source-participant-ambiguous-when-unspecified",
      "No source participant specified and could not infer single source participant for party",
      sourceParticipantO = None,
    )

    testProposalError(
      "bad-synchronizer id",
      "Unknown synchronizer bad::synchronizer",
      synchronizerId = SynchronizerId.tryFromString("bad::synchronizer"),
    )

    testProposalError(
      "unexpected-topology-serial",
      "Specified serial .* does not match the expected serial",
      serialO = Some(PositiveInt.tryCreate(1000)),
    )
  }

  "Prevent malformed party replication agreements" in { implicit env =>
    import env.*
    val participantWithParty = participant1
    val participantWithParty2 = participant2
    val participantWithoutParty = participant3

    import scala.concurrent.duration.*
    // Test the party replication agreement response by directly creating malformed contracts
    // rather than the daml workflow so that the errors are not already caught upon proposal
    // submission.
    def testAgreementError(
        log: String,
        warningRegex: String,
        sourceParticipant: LocalParticipantReference = participantWithParty,
        targetParticipant: LocalParticipantReference = participantWithoutParty,
        sequencerStringUids: Seq[String] = Seq(sequencer2.id.uid.toProtoPrimitive),
        partyIdString: String = alice.toProtoPrimitive,
        serial: Long = unspecifiedTopologySerial,
        partyReplicationIdS: String = validOnPRIdS,
    ) =
      clue(log)(
        loggerFactory.assertEventuallyLogsSeq(SuppressionRule.LevelAndAbove(Level.WARN))(
          {
            val proposal = new M.partyreplication.PartyReplicationProposal(
              partyReplicationIdS,
              partyIdString,
              sourceParticipant.adminParty.toProtoPrimitive,
              targetParticipant.adminParty.toProtoPrimitive,
              sequencerStringUids.asJava,
              serial,
            )
            targetParticipant.ledger_api.commands
              .submit(
                actAs = Seq(targetParticipant.adminParty),
                commands = proposal.create.commands.asScala.toSeq
                  .map(LedgerClientUtils.javaCodegenToScalaProto),
                synchronizerId = Some(daId),
              )
              .discard
          },
          forExactly(1, _)(_.warningMessage should include regex warningRegex),
          timeUntilSuccess = 2.seconds,
        )
      )

    def testProposalExceptionOnEmptySequencerUids(log: String) =
      clue(log)(
        loggerFactory.assertThrowsAndLogs[CommandFailure](
          {
            val proposal = new M.partyreplication.PartyReplicationProposal(
              validOnPRIdS,
              alice.toProtoPrimitive,
              participantWithParty.adminParty.toProtoPrimitive,
              participantWithParty2.adminParty.toProtoPrimitive,
              Seq.empty.asJava,
              unspecifiedTopologySerial,
            )
            participantWithParty2.ledger_api.commands
              .submit(
                actAs = Seq(participantWithParty2.adminParty),
                commands = proposal.create.commands.asScala.toSeq
                  .map(LedgerClientUtils.javaCodegenToScalaProto),
                synchronizerId = Some(daId),
              )
              .discard
          },
          logEntry => {
            logEntry.errorMessage should (include(
              "UNHANDLED_EXCEPTION"
            ) and include(
              "Template precondition violated: PartyReplicationProposal"
            ))
            logEntry.shouldBeCantonErrorCode(CommandExecutionErrors.Interpreter.UnhandledException)
          },
        )
      )

    testAgreementError(
      "bad-party-id-format",
      "Invalid partyId .*Invalid unique identifier .* with missing namespace",
      partyIdString = "bad-party-id",
    )

    testAgreementError(
      "bad-party-replication-id",
      "Invalid party replication id: .*Failed to parse hex string: bad-party-replication-id",
      partyReplicationIdS = "bad-party-replication-id",
    )

    testAgreementError(
      "empty-party-replication-id",
      "Empty party replication id",
      partyReplicationIdS = "",
    )

    testAgreementError(
      "bad-sequencer-uid-format",
      "Invalid unique identifier .* with missing namespace",
      sequencerStringUids = Seq("bad-sequencer-uid"),
    )

    testAgreementError(
      "bad-sequencer-uid",
      "None of the proposed sequencers are active on synchronizer",
      sequencerStringUids = Seq("wrong::sequencer"),
    )

    testProposalExceptionOnEmptySequencerUids("empty-sequencer-uids")

    testAgreementError(
      "source-participant-does-not-host-party",
      "Party .* is not hosted by source participant",
      sourceParticipant = participantWithoutParty,
      targetParticipant = participantWithParty,
    )

    testAgreementError(
      "target-participant-already-hosts-party",
      "Party .* is already hosted by target participant",
      targetParticipant = participantWithParty2,
    )

    testAgreementError(
      "matching-participants",
      "Source and target participants .* cannot match",
      sourceParticipant = participantWithParty,
      targetParticipant = participantWithParty,
    )

    testAgreementError(
      "unexpected-topology-serial",
      "Specified serial .* does not match the expected serial",
      serial = 1000L,
    )

    testAgreementError(
      "out-of-bounds-topology-serial",
      "Non-integer serial",
      serial = Long.MaxValue,
    )

    // Check that all proposals have been archived, i.e. rejected
    Seq(participantWithParty, participantWithParty2, participantWithoutParty).foreach {
      participant =>
        clue(s"participant ${participant.name}: ") {
          eventually() {
            val proposals = participant.ledger_api.state.acs
              .of_all(filterTemplates =
                Seq(
                  TemplateId.fromIdentifier(PartyReplicationAdminWorkflow.proposalTemplate),
                  TemplateId.fromIdentifier(PartyReplicationAdminWorkflow.agreementTemplate),
                )
              )
            proposals shouldBe Seq.empty
          }
        }
    }
  }
}

class OnlinePartyReplicationNegotiationTestPostgres extends OnlinePartyReplicationNegotiationTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
