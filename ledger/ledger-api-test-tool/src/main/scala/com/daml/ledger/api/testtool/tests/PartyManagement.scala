// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import scalaz.Tag

import scala.util.Random

final class PartyManagement(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "PMNonEmptyParticipantID",
    "Asking for the participant identifier should return a non-empty string",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        participantId <- ledger.participantId()
      } yield {
        assert(participantId.nonEmpty, "The ledger returned an empty participant identifier")
      }
  }

  private val pMAllocateWithHint = "PMAllocateWithHint"
  test(
    pMAllocateWithHint,
    "It should be possible to provide a hint when allocating a party",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(
          partyHintId = Some(pMAllocateWithHint + "_" + Random.alphanumeric.take(10).mkString),
          displayName = Some("Bob Ross"),
        )
      } yield assert(
        Tag.unwrap(party).nonEmpty,
        "The allocated party identifier is an empty string",
      )
  }

  test(
    "PMAllocateWithoutHint",
    "It should be possible to not provide a hint when allocating a party",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(partyHintId = None, displayName = Some("Jebediah Kerman"))
      } yield assert(
        Tag.unwrap(party).nonEmpty,
        "The allocated party identifier is an empty string",
      )
  }

  private val pMAllocateWithoutDisplayName = "PMAllocateWithoutDisplayName"
  test(
    pMAllocateWithoutDisplayName,
    "It should be possible to not provide a display name when allocating a party",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        party <- ledger.allocateParty(
          partyHintId =
            Some(pMAllocateWithoutDisplayName + "_" + Random.alphanumeric.take(10).mkString),
          displayName = None,
        )
      } yield assert(
        Tag.unwrap(party).nonEmpty,
        "The allocated party identifier is an empty string",
      )
  }

  test(
    "PMAllocateDuplicateDisplayName",
    "It should be possible to allocate parties with the same display names",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        p1 <- ledger.allocateParty(partyHintId = None, displayName = Some("Ononym McOmonymface"))
        p2 <- ledger.allocateParty(partyHintId = None, displayName = Some("Ononym McOmonymface"))
      } yield {
        assert(Tag.unwrap(p1).nonEmpty, "The first allocated party identifier is an empty string")
        assert(Tag.unwrap(p2).nonEmpty, "The second allocated party identifier is an empty string")
        assert(p1 != p2, "The two parties have the same party identifier")
      }
  }

  test(
    "PMAllocateOneHundred",
    "It should create unique party names when allocating many parties",
    allocate(NoParties),
  ) {
    case Participants(Participant(ledger)) =>
      for {
        parties <- ledger.allocateParties(100)
      } yield {
        val nonUniqueNames = parties.groupBy(Tag.unwrap).mapValues(_.size).filter(_._2 > 1)
        assert(nonUniqueNames.isEmpty, s"There are non-unique party names: ${nonUniqueNames
          .map { case (name, count) => s"$name ($count)" } mkString (", ")}")
      }
  }
}
