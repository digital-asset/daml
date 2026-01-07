// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.BaseTest.CantonLfV21
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{CommandFailure, InstanceReference, ParticipantReference}
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration
import com.digitalasset.canton.integration.EnvironmentDefinition.S1M1
import com.digitalasset.canton.integration.bootstrap.NetworkBootstrapper
import com.digitalasset.canton.integration.plugins.{
  UseBftSequencer,
  UsePostgres,
  UseProgrammableSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.security.SecurityTestHelpers
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  HasCycleUtils,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.party.PartyReplicationTestInterceptorImpl
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.nowarn
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.*

/** Objective: Test repeated party replication done in a cascading fashion such that a target
  * participant in an earlier party replication acts as a source participant in a subsequent party
  * replication. Do so with concurrent archiving of replicated contracts.
  *
  * Also test that OnPR does not slow down mediator-side confirmation response processing in the
  * face of participant local reject verdicts.
  *
  * Setup:
  *   - 6 participants: P1 hosts a decentralized party to replicate to P2, and subsequently from P2
  *     to P3, from P3 to P4, and from P4 to P5.
  *   - party's decentralized namespace is owned by: sequencer1 and mediator1
  *   - 1 mediator/sequencer each
  */
@nowarn("msg=match may not be exhaustive")
sealed trait OnlinePartyReplicationCascadingRepetitionsTest
    extends CommunityIntegrationTest
    with OnlinePartyReplicationTestHelpers
    with SecurityTestHelpers
    with HasCycleUtils // needed by SecurityTestHelpers
    with HasProgrammableSequencer
    with SharedEnvironment {

  registerPlugin(new UseBftSequencer(loggerFactory))
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  // Needed by SecurityTestHelpers:
  private val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference[CryptoPureApi]()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var alice: PartyId = _

  lazy val darPaths: Seq[String] = Seq(CantonLfV21, CantonExamplesPath)

  // false means to block OnPR (temporarily) the moment the SP connects to channel
  private var canSourceProceedWithOnPR: Boolean = false

  // Use the test interceptor to block OnPR until a concurrent exercise is processed by the SP.
  private def createSourceParticipantTestInterceptor() =
    PartyReplicationTestInterceptorImpl.sourceParticipantProceedsIf(_ => canSourceProceedWithOnPR)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition
      .buildBaseEnvironmentDefinition(
        numParticipants = 6,
        numSequencers = 1,
        numMediators = 1,
      )
      .withNetworkBootstrap { implicit env =>
        new NetworkBootstrapper(S1M1)
      }
      .addConfigTransforms(
        ConfigTransforms.unsafeEnableOnlinePartyReplication(
          Map(
            "participant1" -> (() => createSourceParticipantTestInterceptor()),
            "participant2" -> (() => createSourceParticipantTestInterceptor()),
            "participant3" -> (() => createSourceParticipantTestInterceptor()),
            "participant4" -> (() => createSourceParticipantTestInterceptor()),
          )
        )*
      )
      .withSetup { implicit env =>
        import env.*

        updateSynchronizerParameters(
          sequencer1,
          // Lowering max decision timeout speeds up tests that perform multiple, successive OnPRs.
          10.seconds,
          // More aggressive AcsCommitmentProcessor checking.
          _.update(reconciliationInterval = PositiveSeconds.tryOfSeconds(10).toConfig),
        )

        participants.all.synchronizers.connect_local(sequencer1, daName)
        darPaths.foreach(darPath => participants.all.foreach(_.dars.upload(darPath)))

        alice = participant1.parties.enable("alice", synchronizeParticipants = Seq(participant2))

        pureCryptoRef.set(sequencer1.crypto.pureCrypto)
      }

  private var partyOwners: Seq[InstanceReference] = _
  private var decentralizedParty: PartyId = _
  private var previousSerial: PositiveInt = _
  private var dpToAlice: Seq[Iou.Contract] = _
  private var aliceToDp: Seq[Iou.Contract] = _
  private val numContractsInCreateBatch = 200
  private val partyReplications =
    mutable.ArrayDeque[(ParticipantReference, ParticipantReference, Iou.Contract)]()

  "Create decentralized party with contracts" onlyRunWith ProtocolVersion.dev in { implicit env =>
    import env.*

    // last participant only for decentralized namespace owner
    val participantsOrderedByName = participants.all.sortBy(_.name)
    val onPRParticipants = participantsOrderedByName.dropRight(1)

    partyOwners = Seq[InstanceReference](sequencer1, mediator1, participantsOrderedByName.last)
    decentralizedParty = createDecentralizedParty("decentralized-party", partyOwners)

    previousSerial = hostDecentralizedPartyWithCoins(
      decentralizedParty,
      partyOwners,
      participant1,
      numContractsInCreateBatch,
    )

    val amounts = (1 to numContractsInCreateBatch)
    dpToAlice = IouSyntax.createIous(participant1, decentralizedParty, alice, amounts)
    aliceToDp = IouSyntax.createIous(participant1, alice, decentralizedParty, amounts)

    // Chain planned party replications in sliding SP->TP pairs
    onPRParticipants.sliding(2).zip(aliceToDp).foreach {
      case (sp +: tp +: Nil, iouToExerciseDuringOnPR) =>
        partyReplications.append((sp, tp, iouToExerciseDuringOnPR))
    }
  }

  "Replicate party from P1 to P2" onlyRunWith ProtocolVersion.dev in { implicit env =>
    previousSerial = partyReplications.removeHead() match {
      case (sp, tp, iou) => replicateParty(sp, tp, iou, previousSerial.increment)()
    }
  }

  "Replicate party from P2 to P3" onlyRunWith ProtocolVersion.dev in { implicit env =>
    previousSerial = partyReplications.removeHead() match {
      case (sp, tp, iou) => replicateParty(sp, tp, iou, previousSerial.increment)()
    }
  }

  "Replicate party from P3 to P4" onlyRunWith ProtocolVersion.dev in { implicit env =>
    previousSerial = partyReplications.removeHead() match {
      case (sp, tp, iou) => replicateParty(sp, tp, iou, previousSerial.increment)()
    }
  }

  "Issue a request with one rejection while replicating party from P4 to P5" onlyRunWith ProtocolVersion.dev in {
    implicit env =>
      import env.*
      val (sourceParticipant, targetParticipant, _) = partyReplications.removeHead()
      // Make sure we executed all planned party replications
      val aliceParticipant = participant1
      partyReplications shouldBe empty

      // Increase to maximum threshold
      previousSerial = modifyDecentralizedPartyTopology(
        decentralizedParty,
        partyOwners,
        partyOwners,
        { case (node, ptpBefore, serial) =>
          node.topology.party_to_participant_mappings
            .propose(
              party = decentralizedParty,
              newParticipants = ptpBefore.participants.map(hp => hp.participantId -> hp.permission),
              threshold = PositiveInt.tryCreate(ptpBefore.participants.size),
              store = daId,
              serial = Some(serial),
            )
        },
        (ptpBefore, ptpMaxThreshold) =>
          ptpMaxThreshold.threshold.unwrap shouldBe ptpBefore.participants.size,
      )
      val serial = previousSerial.increment

      replicateParty(
        sourceParticipant,
        targetParticipant,
        dpToAlice.head,
        serial,
      ) { () =>
        clue("Exercise with P3 reject produces rejection rather than timeout")(
          loggerFactory.assertThrowsAndLogs[CommandFailure](
            replacingConfirmationResponses(
              senderRef = participant3, // P3 will reject
              sequencerRef = sequencer1,
              synchronizerId = daId,
              messageTransforms = withLocalVerdict(
                LocalRejectError.ConsistencyRejections.LockedContracts
                  .Reject(Seq(dpToAlice.head.id.contractId))
              ),
            )(exerciseIou(aliceParticipant, alice, dpToAlice.head)),
            _.shouldBeCommandFailure(
              LocalRejectError.ConsistencyRejections.LockedContracts,
              "Rejected transaction is referring to locked contracts",
            ),
          )
        )
      }
  }

  private def replicateParty(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      iouToExercise: Iou.Contract,
      serial: PositiveInt,
  )(
      exerciseContract: () => Unit = () =>
        exerciseIou(sourceParticipant, decentralizedParty, iouToExercise)
  )(implicit env: integration.TestConsoleEnvironment): PositiveInt = {
    import env.*
    clue(s"Decentralized party owners agree to have $targetParticipant co-host the party")(
      partyOwners.foreach(
        _.topology.party_to_participant_mappings
          .propose_delta(
            party = decentralizedParty,
            adds = Seq((targetParticipant, ParticipantPermission.Submission)),
            store = daId,
            serial = Some(serial),
            requiresPartyToBeOnboarded = true,
          )
      )
    )

    eventually()(
      partyOwners.map { owner =>
        val p2p = owner.topology.party_to_participant_mappings
          .list(daId, filterParty = decentralizedParty.filterString, proposals = true)
          .loneElement
          .item
        p2p.participants.map(_.participantId) should contain allElementsOf Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
      }
    )

    val addPartyRequestId = clue(s"Initiate add party async to $targetParticipant")(
      targetParticipant.parties.add_party_async(
        party = decentralizedParty,
        synchronizerId = daId,
        sourceParticipant = sourceParticipant,
        serial = serial,
        participantPermission = ParticipantPermission.Submission,
      )
    )

    // Wait until the party is authorized for onboarding on the TP, before archiving replicated contracts.
    canSourceProceedWithOnPR = false
    eventually() {
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
      logger.info(s"Waiting for $targetParticipant to be authorized: $tpStatus")
      tpStatus.authorizationO.nonEmpty shouldBe true
    }

    clue(s"Exercise during replication to TP ${targetParticipant.name}")(exerciseContract())

    logger.info(s"Unblocking progress on SP $sourceParticipant")
    canSourceProceedWithOnPR = true

    // Expect three batches owned by decentralizedParty:
    // 1. all coins plus the coin factory contract (hence the +1 below)
    // 2. Iou batch where decentralizedParty is an observer
    // 3. Iou batch where decentralizedParty is a signatory
    val expectedNumContracts =
      NonNegativeInt.tryCreate(numContractsInCreateBatch * 3 + 1)

    // Wait until both SP and TP report that party replication has completed.
    loggerFactory.assertLogsUnorderedOptional(
      eventuallyOnPRCompletes(
        sourceParticipant,
        targetParticipant,
        addPartyRequestId,
        expectedNumContracts,
        waitAtMost = 3.minutes,
      ),
      // Ignore UNKNOWN status if SP has not found out about the request yet.
      LogEntryOptionality.OptionalMany -> (_.errorMessage should include(
        "UNKNOWN/Add party request id"
      )),
    )

    // Expect all the coins to become indexed and visible via the ledger API.
    eventually() {
      val coinsAtTargetParticipant = CoinFactoryHelpers.getCoins(
        targetParticipant,
        decentralizedParty,
      )
      coinsAtTargetParticipant.size shouldBe numContractsInCreateBatch
    }

    clue(s"Double check that the onboarding flag has been cleared")(
      eventually()((targetParticipant +: partyOwners).map { node =>
        val ptpOnboarded = node.topology.party_to_participant_mappings
          .list(daId, filterParty = decentralizedParty.filterString)
          .loneElement
        ptpOnboarded.item.participants.forall(_.onboarding == false) shouldBe true
        ptpOnboarded.context.serial
      }.head)
    )
  }

  private def exerciseIou(p: ParticipantReference, party: PartyId, iou: Iou.Contract): Unit =
    p.ledger_api.javaapi.commands
      .submit(
        Seq(party),
        iou.id.exerciseCall().commands.asScala.toSeq,
        // cannot wait for other participants because TP indexer paused, so only wait on submitter:
        optTimeout = None,
      )
      .discard
}

// class OnlinePartyReplicationCascadingRepetitionsTestH2
//   extends OnlinePartyReplicationCascadingRepetitionsTest {
//   registerPlugin(new UseH2(loggerFactory))
// }

class OnlinePartyReplicationCascadingRepetitionsTestPostgres
    extends OnlinePartyReplicationCascadingRepetitionsTest {
  registerPlugin(new UsePostgres(loggerFactory))
}
