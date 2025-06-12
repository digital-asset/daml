// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.BigDecimalImplicits.IntToBigDecimal
import com.digitalasset.canton.admin.api.client.data.AddPartyStatus
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{LocalInstanceReference, LocalParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.client.LedgerClientUtils
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Objective: Test party replication of non-local parties such as a decentralized party.
  *
  * Setup:
  *   - 3 participants: participant1 hosts a centralized party to replicate to participant2
  *   - the decentralized party's decentralized namespace is owned by separate nodes: sequencer1,
  *     mediator1, and participant3
  *   - 1 mediator/sequencer each
  */
sealed trait OnlinePartyReplicationDecentralizedPartyTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  private var alice: PartyId = _
  private var bob: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.unsafeEnableOnlinePartyReplication*)
      .withSetup { implicit env =>
        import env.*

        // TODO(#25433): Figure out AcsCommitmentProcessor running-commitment internal consistency check failure after
        //  party replication in spite of indexer pausing on target participant. For now disable ACS commitment checks.
        sequencer1.topology.synchronizer_parameters
          .propose_update(
            daId,
            _.update(reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10).toConfig),
          )

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        alice = participant1.parties.enable("alice", synchronizeParticipants = Seq(participant2))
        bob = participant2.parties.enable("bob", synchronizeParticipants = Seq(participant1))
      }

  private var partyOwners: Seq[LocalInstanceReference] = _
  private var decentralizedParty: PartyId = _
  private var previousSerial: PositiveInt = _
  private val numContractsInCreateBatch = 100

  "Create decentralized party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    partyOwners = Seq[LocalInstanceReference](sequencer1, mediator1, participant3)
    decentralizedParty = createDecentralizedParty("decentralized-party", partyOwners)

    (partyOwners :+ participant1).foreach(
      _.topology.party_to_participant_mappings
        .propose(
          party = decentralizedParty,
          newParticipants = Seq((participant1, ParticipantPermission.Submission)),
          threshold = PositiveInt.one,
          store = daId,
        )
    )

    previousSerial = eventually() {
      val ptpSourceOnly = participant2.topology.party_to_participant_mappings
        .list(daId, filterParty = decentralizedParty.filterString)
        .loneElement
      ptpSourceOnly.item.participants.map(_.participantId) should contain theSameElementsAs Seq(
        participant1.id
      )
      ptpSourceOnly.context.serial
    }

    // Wait until decentralized party is visible via the ledger api on participant1 to ensure that
    // the coin submissions succeed.
    eventually() {
      val partiesOnP1 = participant1.ledger_api.parties.list().map(_.party)
      partiesOnP1 should contain(decentralizedParty)
    }

    logger.info(
      s"Decentralized party created and hosted on source participant $participant1 with serial $previousSerial"
    )

    CoinFactoryHelpers.createCoinsFactory(
      decentralizedParty,
      participant1.adminParty,
      participant1,
    )

    CoinFactoryHelpers.createCoins(
      owner = participant1.adminParty,
      participant = participant1,
      amounts = (1 to numContractsInCreateBatch).map(_.toDouble),
    )

    val amounts = (1 to numContractsInCreateBatch)
    createIous(participant1, alice, decentralizedParty, amounts)
    createIous(participant1, decentralizedParty, alice, amounts)

    // Create some decentralized party stakeholder contracts shared with a party (Bob) already
    // on the target participant P2.
    createIous(participant2, bob, decentralizedParty, amounts)
    createIous(participant1, decentralizedParty, bob, amounts)
  }

  "Replicate a decentralized party" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    val (sourceParticipant, targetParticipant) = (participant1, participant2)

    val serial = previousSerial.increment

    clue("Decentralized party owners agree to have target participant co-host the party")(
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .propose(
            party = decentralizedParty,
            newParticipants = Seq(
              (sourceParticipant, ParticipantPermission.Submission),
              (targetParticipant, ParticipantPermission.Observation),
            ),
            participantsRequiringPartyToBeOnboarded = Seq(targetParticipant),
            threshold = PositiveInt.one,
            store = daId,
            serial = Some(serial),
          )
      )
    )

    eventually() {
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .list(daId, filterParty = decentralizedParty.filterString, proposals = true)
          .flatMap(_.item.participants.map(_.participantId)) shouldBe Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      )
    }

    val addPartyRequestId = clue("Initiate add party async")(
      targetParticipant.parties.add_party_async(
        party = decentralizedParty,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = serial,
      )
    )

    // Wait until the party is authorized for onboarding on the TP, before archiving replicated contracts.
    /* TODO(#25744): Extend LockableStates internal assertVersionedStateIsLatestIfNoPendingWrites to not
         get upset about the OnPR-TP writing directly to the ActiveContractStore. Also figure out how to
         block party replication by tunneling through a hook
    eventually() {
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
      logger.info(s"Waiting until TP has connected: $tpStatus")
      val hasConnected = tpStatus.status match {
        case AddPartyStatus.ProposalProcessed | AddPartyStatus.AgreementAccepted(_) |
            AddPartyStatus.Error(_, _) =>
          false
        case _ =>
          true
      }
      hasConnected shouldBe true
    }
    val iouToExercise = dpToAlice.last
    clue(s"exercise-iou ${iouToExercise.data.amount.value}") {
      sourceParticipant.ledger_api.javaapi.commands
        .submit(Seq(alice), iouToExercise.id.exerciseCall().commands.asScala.toSeq)
        .discard
    }
     */

    // Expect three batches owned by decentralizedParty:
    // 1. all coins plus the coin factory contract (hence the +1 below)
    // 2. Iou batch where decentralizedParty is an observer
    // 3. Iou batch where decentralizedParty is a signatory
    // The "Bob"-batches are not replicated since Bob is already on TP
    val expectedNumContracts = NonNegativeInt.tryCreate(numContractsInCreateBatch * 3 + 1)

    // Wait until both SP and TP report that party replication has completed.
    eventually(retryOnTestFailuresOnly = false, maxPollInterval = 10.millis) {
      val tpStatus = targetParticipant.parties.get_add_party_status(
        addPartyRequestId = addPartyRequestId
      )
      logger.info(s"TP status: $tpStatus")
      val spStatus = loggerFactory.assertLogsUnorderedOptional(
        sourceParticipant.parties.get_add_party_status(
          addPartyRequestId = addPartyRequestId
        ),
        // Ignore UNKNOWN status if SP has not found out about the request yet.
        // Besides logging the error produces a CommandFailure error message, hence
        // the retryOnTestFailuresOnly = false above.
        LogEntryOptionality.Optional -> (_.errorMessage should include(
          "UNKNOWN/Add party request id"
        )),
      )
      logger.info(s"SP status: $spStatus")
      (tpStatus.status, spStatus.status) match {
        case (
              AddPartyStatus.Completed(_, _, `expectedNumContracts`),
              AddPartyStatus.Completed(_, _, `expectedNumContracts`),
            ) =>
          logger.info(
            s"TP and SP completed party replication with status $tpStatus and $spStatus"
          )
        case (
              AddPartyStatus.Completed(_, _, numSpContracts),
              AddPartyStatus.Completed(_, _, numTpContracts),
            ) =>
          logger.warn(
            s"TP and SP completed party replication but had unexpected number of contracts: $numSpContracts and $numTpContracts, expected $expectedNumContracts"
          )
        case (targetStatus, sourceStatus) =>
          fail(
            s"TP and SP did not complete party replication. TP and SP status: $targetStatus and $sourceStatus"
          )
      }
    }

    // Expect all the coins to become indexed and visible via the ledger API.
    eventually() {
      val coinsAtTargetParticipant = CoinFactoryHelpers.getCoins(
        targetParticipant,
        decentralizedParty,
      )
      coinsAtTargetParticipant.size shouldBe numContractsInCreateBatch
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
  }

  private def createDecentralizedParty(
      name: String,
      owners: Seq[LocalInstanceReference],
  )(implicit env: integration.TestConsoleEnvironment): PartyId = {
    import env.*

    val dndResponses =
      owners.map(node =>
        node.topology.decentralized_namespaces.propose_new(
          owners = owners.map(_.namespace).toSet,
          threshold = PositiveInt.tryCreate(owners.size),
          store = daId,
          serial = Some(PositiveInt.one),
        )
      )
    val decentralizedNamespace = dndResponses.head.mapping

    logger.info(
      s"Decentralized namespace ${decentralizedNamespace.namespace} responses: ${dndResponses.mkString(", ")}"
    )

    owners.foreach { owner =>
      utils.retry_until_true(
        owner.topology.decentralized_namespaces
          .list(daId, filterNamespace = decentralizedNamespace.namespace.filterString)
          .exists(_.context.signedBy.forgetNE.toSet == owners.map(_.fingerprint).toSet)
      )
    }

    logger.info(s"Decentralized namespace ${decentralizedNamespace.namespace} authorized")

    PartyId.tryCreate(name, decentralizedNamespace.namespace)
  }

  private def createIous(
      participant: LocalParticipantReference,
      payer: PartyId,
      owner: PartyId,
      amounts: Seq[Int],
  ): Seq[Iou.Contract] = {
    val createIouCmds = amounts.map(amount =>
      new Iou(
        payer.toProtoPrimitive,
        owner.toProtoPrimitive,
        new Amount(amount.toBigDecimal, "USD"),
        List.empty.asJava,
      ).create.commands.loneElement
    )
    clue(s"create ${amounts.size} IOUs") {
      JavaDecodeUtil
        .decodeAllCreated(Iou.COMPANION)(
          participant.ledger_api.javaapi.commands
            .submit(Seq(payer), createIouCmds)
        )
    }
  }
}

class OnlinePartyReplicationDecentralizedPartyTestPostgres
    extends OnlinePartyReplicationDecentralizedPartyTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
