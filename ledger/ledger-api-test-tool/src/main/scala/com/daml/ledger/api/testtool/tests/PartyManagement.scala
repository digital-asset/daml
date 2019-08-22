// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import scalaz.Tag

final class PartyManagement(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val nonEmptyParticipantId =
    LedgerTest(
      "PMNonEmptyParticipantID",
      "Asking for the participant identifier should return a non-empty string") { ledger =>
      ledger
        .participantId()
        .map(id => assert(id.nonEmpty, "The ledger returned an empty participant identifier"))
    }

  private[this] val allocateWithHint =
    LedgerTest(
      "PMAllocateWithHint",
      "It should be possible to provide a hint when allocating a party"
    ) { ledger =>
      for {
        party <- ledger.allocateParty(
          partyHintId = Some("PMAllocateWithHint"),
          displayName = Some("Bob Ross"))
      } yield
        assert(Tag.unwrap(party).nonEmpty, "The allocated party identifier is an empty string")
    }

  private[this] val allocateWithoutHint =
    LedgerTest(
      "PMAllocateWithoutHint",
      "It should be possible to not provide a hint when allocating a party"
    ) { ledger =>
      for {
        party <- ledger.allocateParty(partyHintId = None, displayName = Some("Jebediah Kerman"))
      } yield
        assert(Tag.unwrap(party).nonEmpty, "The allocated party identifier is an empty string")
    }

  private[this] val allocateWithoutDisplayName =
    LedgerTest(
      "PMAllocateWithoutDisplayName",
      "It should be possible to not provide a display name when allocating a party"
    ) { ledger =>
      for {
        party <- ledger.allocateParty(
          partyHintId = Some("PMAllocateWithoutDisplayName"),
          displayName = None)
      } yield
        assert(Tag.unwrap(party).nonEmpty, "The allocated party identifier is an empty string")
    }

  private[this] val allocateTwoPartiesWithTheSameDisplayName =
    LedgerTest(
      "PMAllocateDuplicateDisplayName",
      "It should be possible to allocate parties with the same display names"
    ) { ledger =>
      for {
        p1 <- ledger.allocateParty(partyHintId = None, displayName = Some("Ononym McOmonymface"))
        p2 <- ledger.allocateParty(partyHintId = None, displayName = Some("Ononym McOmonymface"))
      } yield {
        assert(Tag.unwrap(p1).nonEmpty, "The first allocated party identifier is an empty string")
        assert(Tag.unwrap(p2).nonEmpty, "The second allocated party identifier is an empty string")
        assert(p1 != p2, "The two parties have the same party identifier")
      }
    }

  private[this] val allocateOneHundredParties =
    LedgerTest(
      "PMAllocateOneHundred",
      "It should create unique party names when allocating many parties"
    ) { ledger =>
      for {
        parties <- ledger.allocateParties(100)
      } yield {
        val nonUniqueNames = parties.groupBy(Tag.unwrap).filter(_._2.size > 1)
        assert(nonUniqueNames.isEmpty, s"There are non-unique party names: ${nonUniqueNames
          .map { case (name, count) => s"$name ($count)" } mkString (", ")}")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    nonEmptyParticipantId,
    allocateWithHint,
    allocateWithoutHint,
    allocateWithoutDisplayName,
    allocateTwoPartiesWithTheSameDisplayName,
    allocateOneHundredParties
  )
}
