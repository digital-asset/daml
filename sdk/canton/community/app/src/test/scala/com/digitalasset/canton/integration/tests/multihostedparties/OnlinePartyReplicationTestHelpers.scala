// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.admin.api.client.data.AddPartyStatus
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{InstanceReference, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.OnlinePartyReplicationTestHelpers.PreparedOnPRSetup
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.{
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.{BaseTest, integration}

import scala.concurrent.duration.*
import scala.util.Try

/** Utilities for testing online party replication.
  */
private[tests] trait OnlinePartyReplicationTestHelpers {
  this: BaseTest =>

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
    */
  protected def eventuallyOnPRCompletes(
      sourceParticipant: ParticipantReference,
      targetParticipant: ParticipantReference,
      addPartyRequestId: String,
      expectedNumContracts: NonNegativeInt,
  ): Unit =
    eventually(retryOnTestFailuresOnly = false, maxPollInterval = 10.millis) {
      // The try handles the optional `CommandFailure`, so that we don't give up while the SP is stopped.
      val spStatusO =
        Try(sourceParticipant.parties.get_add_party_status(addPartyRequestId)).toOption
      val tpStatus = targetParticipant.parties.get_add_party_status(addPartyRequestId)
      (spStatusO.map(_.status), tpStatus.status) match {
        case (
              Some(spStatus @ AddPartyStatus.Completed(_, _, `expectedNumContracts`)),
              AddPartyStatus.Completed(_, _, `expectedNumContracts`),
            ) =>
          logger.info(
            s"SP and TP completed party replication with status $spStatus and $tpStatus"
          )
        case (
              Some(AddPartyStatus.Completed(_, _, numSpContracts)),
              AddPartyStatus.Completed(_, _, numTpContracts),
            ) =>
          logger.warn(
            s"SP and TP completed party replication but had unexpected number of contracts: $numSpContracts and $numTpContracts, expected $expectedNumContracts"
          )
        case (sourceStatusO, targetStatus) =>
          fail(
            s"TP and SP did not complete party replication. SP and TP status: $sourceStatusO and $targetStatus"
          )
      }
    }
}

private[tests] object OnlinePartyReplicationTestHelpers {
  final case class PreparedOnPRSetup(topologySerial: PositiveInt, expectedNumContracts: PositiveInt)
}
