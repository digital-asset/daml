// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.digitalasset.canton.admin.api.client.data.AddPartyStatus
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.multihostedparties.OnlinePartyReplicationTestHelpers.PreparedOnPRSetup
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
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
