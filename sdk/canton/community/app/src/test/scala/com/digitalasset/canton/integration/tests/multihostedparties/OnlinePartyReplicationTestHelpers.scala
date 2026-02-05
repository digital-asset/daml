// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.admin.api.client.data.DynamicSynchronizerParameters as ConsoleDynamicSynchronizerParameters
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{
  DebuggingHelpers,
  InstanceReference,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.OnlinePartyReplicationTestHelpers.{
  AuthorizedOnPRSetup,
  PreparedOnPRSetup,
}
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.participant.admin.data.PartyReplicationStatus
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{ExternalParty, Party, PartyId}
import com.digitalasset.canton.{BaseTest, config, integration}
import org.scalatest.Assertion

import scala.annotation.nowarn
import scala.concurrent.duration.*
import scala.util.Try

/** Utilities for testing online party replication.
  */
private[tests] trait OnlinePartyReplicationTestHelpers {
  this: BaseTest =>

  /** Authorize the onboarding of the specified external party from SP to TP.
    *
    * @param sourceParticipant
    *   the source participant to export the ACS from
    * @param targetParticipant
    *   the target participant to onboard the party and import the ACS to
    * @param partyToReplicate
    *   the external party to replicate
    * @return
    *   AuthorizedOnPRSetup with parameters needed for file-based ACS export and import
    */
  protected def authorizeOnboardingTopologyForFileBasedOnPR(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      partyToReplicate: ExternalParty,
  )(implicit
      env: integration.TestConsoleEnvironment
  ): AuthorizedOnPRSetup = {
    import env.*
    val spOffsetBeforePartyAddedToTargetParticipant = sourceParticipant.ledger_api.state.end()

    clue("External party and TP agree to have target participant onboard the party")(
      PartyToParticipantDeclarative(participants.all.toSet, Set(daId))(
        owningParticipants = Map.empty,
        targetTopology = Map(
          partyToReplicate -> Map(
            daId -> (PositiveInt.one, Set(
              (sourceParticipant, ParticipantPermission.Confirmation),
              (targetParticipant, ParticipantPermission.Confirmation),
            ))
          )
        ),
        onboarding = true,
      )
    )

    val serial = eventually() {
      participants.all.map { p =>
        val res = p.topology.party_to_participant_mappings
          .list(daId, filterParty = partyToReplicate.filterString)
          .loneElement
        res.item.participants.map(_.participantId) shouldBe Seq(
          sourceParticipant.id,
          targetParticipant.id,
        )
        res.context.serial
      }
    }.head

    AuthorizedOnPRSetup(serial, spOffsetBeforePartyAddedToTargetParticipant)
  }

  protected def updateSynchronizerParameters(
      synchronizerOwner: InstanceReference,
      maxDecisionTimeout: config.NonNegativeFiniteDuration,
      update: ConsoleDynamicSynchronizerParameters => ConsoleDynamicSynchronizerParameters =
        identity,
  )(implicit
      env: integration.TestConsoleEnvironment
  ): Unit = {
    import env.*
    val half = (maxDecisionTimeout.toInternal / NonNegativeInt.two).toConfig
    synchronizerOwner.topology.synchronizer_parameters.propose_update(
      daId,
      params =>
        update(params.update(confirmationResponseTimeout = half, mediatorReactionTimeout = half)),
      mustFullyAuthorize = true,
    )
  }

  protected def createSharedContractsAndProposeTopologyForOnPR(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      partyToReplicate: PartyId,
      fellowContractStakeholder: PartyId,
      numContractsInCreateBatch: PositiveInt,
  )(implicit env: integration.TestConsoleEnvironment): PreparedOnPRSetup = {
    import env.*

    // Expect both batches owned by the replicated party.
    val expectedNumContracts = numContractsInCreateBatch * 2
    val amounts = (1 to numContractsInCreateBatch.unwrap)
    clue(s"create ${expectedNumContracts.unwrap} IOUs") {
      IouSyntax.createIous(sourceParticipant, partyToReplicate, fellowContractStakeholder, amounts)
      IouSyntax.createIous(sourceParticipant, fellowContractStakeholder, partyToReplicate, amounts)
    }

    val serial = clue(s"$partyToReplicate agrees to have target participant co-host them")(
      sourceParticipant.topology.party_to_participant_mappings
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

    PreparedOnPRSetup(serial, expectedNumContracts)
  }

  /** Wait for 5 seconds to ensure the disruption produces a noticed "outage" longer than a blip */
  protected def sleepLongEnoughForDisruptionToBeNoticed(): Unit = Threading.sleep(5000)

  /** Create a party in a decentralized namespace owned by the specified owners.
    */
  protected def createDecentralizedParty(
      partyName: String,
      owners: Seq[InstanceReference],
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

    PartyId.tryCreate(partyName, decentralizedNamespace.namespace)
  }

  /** Host a decentralized party on the specified participant waiting until all participants see it,
    * and issue the specified number of coins to the decentralized party. Returns the topology
    * serial after the party has been hosted.
    */
  protected def hostDecentralizedPartyWithCoins(
      decentralizedParty: PartyId,
      partyOwners: Seq[InstanceReference],
      participantHostDp: ParticipantReference,
      numCoins: Int,
  )(implicit env: integration.TestConsoleEnvironment): PositiveInt = {
    import env.*
    (partyOwners :+ participantHostDp).foreach(
      _.topology.party_to_participant_mappings
        .propose(
          party = decentralizedParty,
          newParticipants = Seq((participantHostDp, ParticipantPermission.Submission)),
          threshold = PositiveInt.one,
          store = daId,
        )
    )

    // Wait until all participants see the party hosted on the expected participant.
    val serial = eventually() {
      participants.all
        .map { p =>
          val ptp = p.topology.party_to_participant_mappings
            .list(daId, filterParty = decentralizedParty.filterString)
            .loneElement
          ptp.item.participants.map(_.participantId) should contain theSameElementsAs Seq(
            participantHostDp.id
          )
          ptp.context.serial
        }
        .toSet
        .loneElement
    }

    // Wait until decentralized party is visible via the ledger api on the expected participant
    // to ensure that the coin submissions succeed.
    eventually() {
      val partiesOnP1 = participantHostDp.ledger_api.parties.list().map(_.party)
      partiesOnP1 should contain(decentralizedParty)
    }

    logger.info(s"Decentralized party hosted on participant $participantHostDp with serial $serial")

    // Issue a ping to ensure that the RoutingSynchronizerState does not pick a stale
    // topology snapshot that does not yet know about the decentralized party (#25474).
    participantHostDp.health.ping(participantHostDp)

    CoinFactoryHelpers.createCoinsFactory(
      decentralizedParty,
      participantHostDp.adminParty,
      participantHostDp,
    )

    CoinFactoryHelpers.createCoins(
      owner = participantHostDp.adminParty,
      participant = participantHostDp,
      amounts = (1 to numCoins).map(_.toDouble),
    )

    serial
  }

  /** Helper to modify the PartyToParticipant topology in contexts in which only the party owners
    * need to approve.
    */
  protected def modifyDecentralizedPartyTopology(
      decentralizedParty: PartyId,
      authorizers: Seq[InstanceReference],
      verifiers: Seq[InstanceReference],
      propose: (
          InstanceReference,
          PartyToParticipant,
          PositiveInt,
      ) => SignedTopologyTransaction[TopologyChangeOp, PartyToParticipant],
      verifyBeforeAfter: (PartyToParticipant, PartyToParticipant) => Unit,
  )(implicit env: integration.TestConsoleEnvironment): PositiveInt = {
    import env.*
    verifiers should contain allElementsOf authorizers
    val ptpCurrent = authorizers.head.topology.party_to_participant_mappings
      .list(daId, filterParty = decentralizedParty.filterString)
      .loneElement
    val serial = ptpCurrent.context.serial.increment

    authorizers.foreach(propose(_, ptpCurrent.item, serial).discard)

    eventually() {
      verifiers.foreach(po =>
        verifyBeforeAfter(
          ptpCurrent.item,
          po.topology.party_to_participant_mappings
            .list(daId, filterParty = decentralizedParty.filterString)
            .loneElement
            .item,
        )
      )
    }

    serial
  }

  /** Wait until online party replication completes on the source and target participants with the
    * expected number of replicated contracts on the specified request.
    *
    * @param waitAtMost
    *   Clearing the onboarding flag takes up to max-decision-timeout (initial value of 60s), so
    *   wait at least 1 minute at the very least. Also figure about 1 minute for 300 contracts, e.g.
    *   2 minutes total for an ACS size of 600 contracts plus an extra minute to avoid flakiness.
    */
  protected def eventuallyOnPRCompletes(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      addPartyRequestId: String,
      expectedNumContracts: NonNegativeInt,
      waitAtMost: FiniteDuration = 2.minutes, // default enough for ~400 contracts
  ): Unit = eventuallyOnPRCompletes(
    sourceParticipant,
    targetParticipant,
    addPartyRequestId,
    Some(expectedNumContracts),
    waitAtMost,
  )

  protected def eventuallyOnPRCompletes(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      addPartyRequestId: String,
      expectedNumContractsO: Option[NonNegativeInt],
      waitAtMost: FiniteDuration,
  ): Unit =
    eventually(
      timeUntilSuccess = waitAtMost,
      retryOnTestFailuresOnly = false,
      maxPollInterval = 1.second,
    ) {
      // The try handles the optional `CommandFailure`, so that we don't give up while the SP is stopped.
      val spStatusO =
        Try(sourceParticipant.parties.get_add_party_status(addPartyRequestId)).toOption
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)

      (spStatusO, tpStatus) match {
        case (Some(spStatus), tp)
            if countsMatch(spStatus, expectedNumContractsO) && countsMatch(
              tp,
              expectedNumContractsO,
            ) =>
          logger.info(
            s"SP and TP completed party replication with status $spStatus and $tpStatus"
          )
        case (Some(spStatus), tp) if finished(spStatus) && finished(tp) =>
          logger.warn(
            s"SP and TP completed party replication but had unexpected number of contracts: $spStatus and $tpStatus, expected $expectedNumContractsO"
          )
        case (sourceStatusO, targetStatus) =>
          fail(
            s"TP and SP did not complete party replication. SP and TP status: $sourceStatusO and $targetStatus"
          )
      }
    }

  protected def eventuallyOnPRCompletesOnTP(
      targetParticipant: ParticipantReference,
      addPartyRequestId: String,
      expectedNumContractsO: Option[NonNegativeInt],
      waitAtMost: FiniteDuration = 2.minutes, // default enough for ~400 contracts
  ): Unit =
    eventually(
      timeUntilSuccess = waitAtMost,
      retryOnTestFailuresOnly = false,
      maxPollInterval = 1.second,
    ) {
      targetParticipant.parties.get_add_party_status(addPartyRequestId) match {
        case tpStatus if countsMatch(tpStatus, expectedNumContractsO) =>
          logger.info(s"TP completed party replication with status $tpStatus")
        case tpStatus if finished(tpStatus) =>
          logger.warn(
            s"TP completed party replication but had unexpected number of contracts: $tpStatus, expected $expectedNumContractsO"
          )
        case tpStatus =>
          fail(s"TP did not complete party replication. TP status $tpStatus")
      }
    }

  private def finished(status: PartyReplicationStatus) =
    status.hasCompleted && status.errorO.isEmpty
  private def countsMatch(
      status: PartyReplicationStatus,
      expectedNumContractsO: Option[NonNegativeInt],
  ) = finished(status) && status.replicationO.exists(progress =>
    progress.fullyProcessedAcs && expectedNumContractsO.forall(progress.processedContractCount == _)
  )

  @nowarn("msg=match may not be exhaustive")
  protected def eventuallyLedgerApiAcsInSyncBetweenSPAndTP(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      replicatedParty: Party,
      limit: PositiveInt,
  ): Assertion =
    eventually() {
      val Seq(acsSP, acsTP) = Seq(sourceParticipant, targetParticipant).map(
        _.ledger_api.state.acs
          .of_party(replicatedParty, limit, verbose = false)
          .map(entry => entry.contractId -> entry)
          .toMap
      )

      val missingFromTP = acsSP -- acsTP.keySet
      val missingFromSP = acsTP -- acsSP.keySet

      assert(
        missingFromTP.isEmpty,
        s"These ${missingFromTP.size} contracts are missing from the TP: $missingFromTP",
      )
      assert(
        missingFromSP.isEmpty,
        s"These ${missingFromSP.size} contracts are missing from the SP: $missingFromSP",
      )
    }

  protected def ensureActiveContractsInSyncBetweenLedgerApiAndSyncService(
      participant: LocalParticipantReference,
      limit: PositiveInt,
  ): Assertion = {
    val (syncAcs, lapiAcs) =
      DebuggingHelpers.get_active_contracts_from_internal_db_state(
        participant,
        participant.testing.state_inspection,
        limit,
      )

    val missingFromLapi = syncAcs.keySet.diff(lapiAcs.keySet)
    val missingFromSync = lapiAcs.keySet.diff(syncAcs.keySet)

    val mismatchErrors = Option
      .when(missingFromLapi.nonEmpty)(
        s"contracts missing from Ledger API: ${missingFromLapi.mkString(", ")}"
      )
      .toList ++ Option
      .when(missingFromSync.nonEmpty)(
        s"contracts missing from sync state: ${missingFromSync.mkString(", ")}"
      )
      .toList

    mismatchErrors shouldBe List.empty
  }
}

private[tests] object OnlinePartyReplicationTestHelpers {
  final case class PreparedOnPRSetup(topologySerial: PositiveInt, expectedNumContracts: PositiveInt)
  final case class AuthorizedOnPRSetup(
      topologySerial: PositiveInt,
      spOffsetBeforePartyOnboardingToTargetParticipant: Long,
  )
}
