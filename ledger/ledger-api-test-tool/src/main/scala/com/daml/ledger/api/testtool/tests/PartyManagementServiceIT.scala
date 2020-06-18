// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.{Assertions, LedgerTestSuite}
import com.daml.ledger.api.v1.admin.party_management_service.PartyDetails
import com.daml.ledger.client.binding
import com.daml.lf.data.Ref
import scalaz.Tag

import scala.util.Random

final class PartyManagementServiceIT extends LedgerTestSuite {
  test(
    "PMNonEmptyParticipantID",
    "Asking for the participant identifier should return a non-empty string",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        participantId <- ledger.participantId()
      } yield {
        assert(participantId.nonEmpty, "The ledger returned an empty participant identifier")
      }
  })

  private val pMAllocateWithHint = "PMAllocateWithHint"
  test(
    pMAllocateWithHint,
    "It should be possible to provide a hint when allocating a party",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(
          partyIdHint = Some(pMAllocateWithHint + "_" + Random.alphanumeric.take(10).mkString),
          displayName = Some("Bob Ross"),
        )
      } yield
        assert(
          Tag.unwrap(party).nonEmpty,
          "The allocated party identifier is an empty string",
        )
  })

  test(
    "PMAllocateWithoutHint",
    "It should be possible to not provide a hint when allocating a party",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(partyIdHint = None, displayName = Some("Jebediah Kerman"))
      } yield
        assert(
          Tag.unwrap(party).nonEmpty,
          "The allocated party identifier is an empty string",
        )
  })

  private val pMAllocateWithoutDisplayName = "PMAllocateWithoutDisplayName"
  test(
    pMAllocateWithoutDisplayName,
    "It should be possible to not provide a display name when allocating a party",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(
          partyIdHint =
            Some(pMAllocateWithoutDisplayName + "_" + Random.alphanumeric.take(10).mkString),
          displayName = None,
        )
      } yield
        assert(
          Tag.unwrap(party).nonEmpty,
          "The allocated party identifier is an empty string",
        )
  })

  test(
    "PMAllocateDuplicateDisplayName",
    "It should be possible to allocate parties with the same display names",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        p1 <- ledger.allocateParty(partyIdHint = None, displayName = Some("Ononym McOmonymface"))
        p2 <- ledger.allocateParty(partyIdHint = None, displayName = Some("Ononym McOmonymface"))
      } yield {
        assert(Tag.unwrap(p1).nonEmpty, "The first allocated party identifier is an empty string")
        assert(Tag.unwrap(p2).nonEmpty, "The second allocated party identifier is an empty string")
        assert(p1 != p2, "The two parties have the same party identifier")
      }
  })

  test(
    "PMAllocateOneHundred",
    "It should create unique party names when allocating many parties",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        parties <- ledger.allocateParties(100)
      } yield {
        val nonUniqueNames = parties.groupBy(Tag.unwrap).mapValues(_.size).filter(_._2 > 1)
        assert(nonUniqueNames.isEmpty, s"There are non-unique party names: ${nonUniqueNames
          .map { case (name, count) => s"$name ($count)" }
          .mkString(", ")}")
      }
  })

  test(
    "PMGetParties",
    "It should get details for multiple parties, if they exist",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        party1 <- ledger.allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          displayName = Some("Alice"),
        )
        party2 <- ledger.allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          displayName = Some("Bob"),
        )
        partyDetails <- ledger.getParties(
          Seq(party1, party2, binding.Primitive.Party("non-existent")))
        noPartyDetails <- ledger.getParties(Seq(binding.Primitive.Party("non-existent")))
        zeroPartyDetails <- ledger.getParties(Seq.empty)
      } yield {
        assert(
          partyDetails.sortBy(_.displayName) == Seq(
            PartyDetails(
              party = Ref.Party.assertFromString(Tag.unwrap(party1)),
              displayName = "Alice",
              isLocal = true,
            ),
            PartyDetails(
              party = Ref.Party.assertFromString(Tag.unwrap(party2)),
              displayName = "Bob",
              isLocal = true,
            ),
          ),
          s"The allocated parties, ${Seq(party1, party2)}, were not retrieved successfully. Instead, got $partyDetails."
        )
        assert(
          noPartyDetails.isEmpty,
          s"Retrieved some parties when the party specified did not exist: $noPartyDetails")
        assert(
          zeroPartyDetails.isEmpty,
          s"Retrieved some parties when no parties were requested: $zeroPartyDetails")
      }
  })

  test(
    "PMListKnownParties",
    "It should list all known, previously-allocated parties",
    allocate(NoParties),
  )(implicit ec => {
    case Participants(Participant(ledger)) =>
      for {
        party1 <- ledger.allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          displayName = None,
        )
        party2 <- ledger.allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          displayName = None,
        )
        party3 <- ledger.allocateParty(
          partyIdHint = Some("PMListKnownParties_" + Random.alphanumeric.take(10).mkString),
          displayName = None,
        )
        knownPartyIds <- ledger.listKnownParties()
      } yield {
        val allocatedPartyIds = Set(party1, party2, party3)
        assert(
          allocatedPartyIds subsetOf knownPartyIds,
          s"The allocated party IDs $allocatedPartyIds are not a subset of $knownPartyIds.")
      }
  })

  test(
    "PMGetPartiesIsLocal",
    "Getting parties on multi-node ledgers should reflect whether parties are local",
    allocate(NoParties, NoParties),
  )(implicit ec => {
    case Participants(Participant(alpha), Participant(beta)) =>
      val alphaPartyHintAndDisplayName =
        "PMGetPartiesIsLocal_" + Random.alphanumeric.take(10).mkString
      val betaPartyHintAndDisplayName =
        "PMGetPartiesIsLocal_" + Random.alphanumeric.take(10).mkString
      for {
        alphaLocalParty <- alpha.allocateParty(
          partyIdHint = Some(alphaPartyHintAndDisplayName),
          displayName = Some(alphaPartyHintAndDisplayName),
        )
        betaLocalParty <- beta.allocateParty(
          partyIdHint = Some(betaPartyHintAndDisplayName),
          displayName = Some(betaPartyHintAndDisplayName),
        )
        // only wait for parties on ledgers configured to know about each other's parties
        _ <- alpha.waitForParties(Set(alpha, beta), Set(alphaLocalParty, betaLocalParty))
        _ <- beta.waitForParties(Set(alpha, beta), Set(alphaLocalParty, betaLocalParty))
        alphaPartyOnAlphaParticipant <- alpha.getParties(Seq(alphaLocalParty))
        alphaPartyOnBetaParticipant <- beta.getParties(Seq(alphaLocalParty))
        betaPartyOnAlphaParticipant <- alpha.getParties(Seq(betaLocalParty))
        betaPartyOnBetaParticipant <- beta.getParties(Seq(betaLocalParty))
      } yield {
        alphaPartyOnAlphaParticipant.headOption match {
          case None =>
            Assertions.fail("Expected to find alpha party on alpha participant")
          case Some(alphaLocal) =>
            assert(alphaLocal.displayName == alphaPartyHintAndDisplayName && alphaLocal.isLocal)
        }

        betaPartyOnBetaParticipant.headOption match {
          case None =>
            Assertions.fail("Expected to find beta party on beta participant")
          case Some(betaLocal) =>
            assert(betaLocal.displayName == betaPartyHintAndDisplayName && betaLocal.isLocal)
        }

        // On multi-participant ledgers remote parties should either not be known to each
        // other or should be marked as non-local.
        if (alpha.endpointId != beta.endpointId) {
          alphaPartyOnBetaParticipant.headOption match {
            // distributed ledgers may choose not to publish parties across participants (e.g. privacy-enabled P-KVUtils)
            case None =>
            // but if they do, remote parties have to be marked non-local
            case Some(alphaRemote) =>
              assert(
                alphaRemote.displayName == alphaPartyHintAndDisplayName && !alphaRemote.isLocal)
          }

          betaPartyOnAlphaParticipant.headOption match {
            // distributed ledgers may choose not to publish parties across participants (e.g. privacy-enabled P-KVUtils)
            case None =>
            // but if they do, remote parties have to be marked non-local
            case Some(betaRemote) =>
              assert(betaRemote.displayName == betaPartyHintAndDisplayName && !betaRemote.isLocal)
          }
        }
      }
  })

}
