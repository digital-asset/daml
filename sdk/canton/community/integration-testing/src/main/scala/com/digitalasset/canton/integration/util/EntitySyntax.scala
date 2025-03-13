// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.console.ConsoleEnvironment.Implicits.*
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission

/** Additional syntactic sugar for synchronizers, participants, parties, and packages
  */
trait EntitySyntax {
  this: BaseTest =>

  val defaultParticipant: String = "participant1"

  implicit class ParticipantReferenceSyntax(participantReference: ParticipantReference)(implicit
      env: TestConsoleEnvironment
  ) {

    def ownParties(filterSynchronizerId: Option[SynchronizerId] = None): Set[PartyId] =
      participantReference.parties
        .hosted(synchronizerIds = filterSynchronizerId.toList.toSet)
        .map(_.party)
        .toSet

    def authorizePartyOnParticipant(
        partyId: PartyId,
        to: ParticipantReference,
        synchronizerId: SynchronizerId,
        permission: ParticipantPermission = ParticipantPermission.Submission,
        participantsToWait: Seq[LocalParticipantReference] = env.participants.local,
    ): Unit = {

      // Check that the Participant `to` does not yet represent `partyId`.
      to.ownParties() should not contain partyId

      participantReference.topology.party_to_participant_mappings.propose_delta(
        partyId,
        adds = List(to.id -> permission),
        store = synchronizerId,
      )

      to.topology.party_to_participant_mappings.propose_delta(
        partyId,
        adds = List(to.id -> permission),
        store = synchronizerId,
      )

      for (p <- participantsToWait)
        eventually() {
          partyId.participants(p, synchronizerId) should contain(to.id)
        }
    }
  }

  implicit class PartyIdSyntax(partyId: PartyId)(implicit env: TestConsoleEnvironment) {
    def participants(
        requestingParticipant: LocalParticipantReference,
        synchronizerId: Option[SynchronizerId] = None,
    ): Set[ParticipantId] =
      requestingParticipant.parties
        .list(filterParty = partyId.filterString, synchronizerIds = synchronizerId.toList.toSet)
        .flatMap(_.participants.map(_.participant))
        .toSet
  }

  implicit class StringConversions(name: String)(implicit env: TestConsoleEnvironment) {
    import env.*

    def toPartyId(
        requestingParticipant: ParticipantReference = defaultParticipant.toParticipantRef
    ): PartyId = {
      // Query party
      val candidateParties =
        requestingParticipant.topology.party_to_participant_mappings
          .list_from_all(filterParty = name + UniqueIdentifier.delimiter)
          .map(_.item.partyId)
          .toSet

      // Create result
      withClue(s"extracting the party with name $name on participant $requestingParticipant") {
        candidateParties.loneElement
      }
    }

    def toParticipantRef: LocalParticipantReference = lp(name)
  }

}
