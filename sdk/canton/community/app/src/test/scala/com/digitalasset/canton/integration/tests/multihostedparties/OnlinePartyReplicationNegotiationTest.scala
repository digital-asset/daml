// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.event.Event.Event.{Created, Exercised}
import com.daml.ledger.api.v2.event.{CreatedEvent, Event}
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.api.v2.value.Value.Sum.Party
import com.daml.ledger.api.v2.value.{RecordField, Value}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.admin.api.client.data.{AddPartyStatus, TemplateId}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{DbConfig, SynchronizerTimeTrackerConfig}
import com.digitalasset.canton.console.{
  CommandFailure,
  InstanceReference,
  LocalInstanceReference,
  LocalParticipantReference,
}
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
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
  SequencerConnectionPoolDelays,
  SequencerConnectionValidation,
  SequencerConnections,
  SubmissionRequestAmplification,
}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeConfig
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{SequencerAlias, config}
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

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

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private val aliceName = "Alice"

  private var alice: PartyId = _

  private val channelServiceNotImplementedWarning =
    "GrpcServiceUnavailable: UNIMPLEMENTED/Method not found: .*SequencerChannelService/Ping"

  private val validOnPRIdS = Hash
    .build(HashPurpose.OnlinePartyReplicationId, HashAlgorithm.Sha256)
    .finish()
    .toHexString

  private val dummyTopologySerial = 1L

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
        // TODO(#24326): While the SourceParticipant (SP=P1) uses AcsInspection to consume the
        //  ACS snapshot (rather than the Ledger Api), ensure ACS pruning does not trigger AcsInspection
        //  TimestampBeforePruning. Allow a generous 5 minutes for the SP to consume all active contracts
        //  in this test.
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(config.NonNegativeFiniteDuration.ofMinutes(5))
        ),
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
            // A threshold of 3 ensures that each participant connects to all the three sequencers in the connectivity map
            // and stays connected.
            // TODO(#19911) Make this properly configurable
            sequencerTrustThreshold = PositiveInt.three,
            sequencerLivenessMargin = NonNegativeInt.zero,
            submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
            sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
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

  "Obtain agreement to establish to replicate a party" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val (sourceParticipant, targetParticipant) = (participant1, participant2)

      val serial = PositiveInt.two

      val partyOwners = Seq[LocalInstanceReference](sourceParticipant)
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            party = alice,
            adds = Seq((targetParticipant, ParticipantPermission.Confirmation)),
            store = daId,
            serial = Some(serial),
            requiresPartyToBeOnboarded = true,
          )
      )
      eventually() {
        partyOwners.foreach(
          _.topology.party_to_participant_mappings
            .list(daId, filterParty = alice.filterString, proposals = true)
            .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
            sourceParticipant.id,
            targetParticipant.id,
          )
        )
      }

      val addPartyRequestId =
        clue("Initiate add party async")(
          targetParticipant.parties.add_party_async(
            party = alice,
            synchronizerId = daId,
            sourceParticipant = sourceParticipant,
            serial = serial,
            participantPermission = ParticipantPermission.Confirmation,
          )
        ).tap { _ =>
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
                val txs = participant.ledger_api.updates.transactions(
                  Set(sourceParticipant.adminParty),
                  completeAfter = 10,
                  timeout = config.NonNegativeDuration.ofSeconds(1),
                  transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
                )

                // The TP asks the SP to replicate Alice via proposal
                val createProposal = txs.flatMap { case TransactionWrapper(tx: Transaction) =>
                  tx.events.collect {
                    case Event(Created(event))
                        if event.templateId.contains(
                          PartyReplicationAdminWorkflow.proposalTemplate
                        ) =>
                      event
                  }
                }.loneElement
                val party = partyToReplicate(createProposal)
                createProposal.signatories shouldBe Seq(
                  targetParticipant.adminParty.toProtoPrimitive
                )
                party shouldBe alice.toProtoPrimitive

                // The SP accepts the party replication proposal
                val accept = txs
                  .collect { case TransactionWrapper(tx: Transaction) =>
                    tx.events.collect {
                      case Event(Exercised(event))
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
              }
            }
          }
        }

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

      // Wait until both SP and TP report that party replication has completed.
      // Clearing the onboarding flag takes up to max-decision-timeout (initial value of 60s),
      // so wait at least 1 minute.
      eventually(timeUntilSuccess = 2.minutes) {
        val tpStatus = targetParticipant.parties.get_add_party_status(
          addPartyRequestId = addPartyRequestId
        )
        val spStatus = sourceParticipant.parties.get_add_party_status(
          addPartyRequestId = addPartyRequestId
        )
        logger.info(s"TP status: $tpStatus")
        logger.info(s"SP status: $spStatus")
        assert(
          tpStatus.status.isInstanceOf[AddPartyStatus.Completed],
          "Target participant must complete",
        )
        assert(
          spStatus.status.isInstanceOf[AddPartyStatus.Completed],
          "Source participant must complete",
        )
      }
  }

  "Prevent malformed party replication proposals" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val participantWithParty = participant1
      val participantWithParty2 = participant2
      val participantWithoutParty = participant3

      def testProposalError(
          log: String,
          errorRegex: String,
          targetParticipant: LocalParticipantReference = participantWithoutParty,
          sourceParticipant: LocalParticipantReference = participantWithParty2,
          synchronizerId: SynchronizerId = daId,
          serial: PositiveInt = PositiveInt.one,
      ): Unit =
        clue(log)(
          loggerFactory.assertThrowsAndLogsUnorderedOptional[CommandFailure](
            targetParticipant.parties.add_party_async(
              party = alice,
              synchronizerId = synchronizerId,
              sourceParticipant = sourceParticipant.id,
              serial = serial,
              participantPermission = ParticipantPermission.Observation,
            ),
            LogEntryOptionality.Required -> (_.errorMessage should include regex errorRegex),
            LogEntryOptionality.Optional -> (_.warningMessage should include regex channelServiceNotImplementedWarning),
          )
        )

      testProposalError(
        "source-participant-does-not-host-party",
        "Party .* is not hosted by source participant",
        sourceParticipant = participantWithoutParty,
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
        sourceParticipant = participantWithParty,
        targetParticipant = participantWithParty,
      )

      testProposalError(
        "bad-synchronizer id",
        "Unknown synchronizer bad::synchronizer",
        synchronizerId = SynchronizerId.tryFromString("bad::synchronizer"),
      )

      testProposalError(
        "unexpected-topology-serial",
        "Specified serial .* does not match the expected serial",
        serial = PositiveInt.tryCreate(1000),
      )
  }

  "Prevent malformed party replication agreements" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
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
          serial: Long = dummyTopologySerial,
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
                M.partyreplication.ParticipantPermission.CONFIRMATION,
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
                dummyTopologySerial,
                M.partyreplication.ParticipantPermission.CONFIRMATION,
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
                "Interpretation error: Error: User failure:"
              ) and include(
                "Template precondition violated: PartyReplicationProposal"
              ))
              logEntry.shouldBeCantonErrorCode(
                CommandExecutionErrors.Interpreter.UnhandledException
              )
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
                    TemplateId.fromIdentifier(
                      PartyReplicationAdminWorkflow.proposalTemplatePkgName
                    ),
                    TemplateId.fromIdentifier(
                      PartyReplicationAdminWorkflow.agreementTemplatePkgName
                    ),
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
