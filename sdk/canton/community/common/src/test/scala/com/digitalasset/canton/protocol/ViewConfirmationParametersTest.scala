// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.TransactionViewDecompositionFactory.withSubmittingAdminParty
import com.digitalasset.canton.data.{Quorum, ViewConfirmationParameters}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

class ViewConfirmationParametersTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val gen = new ExampleTransactionFactory()()

  private lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  private lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")
  private lazy val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  private lazy val david: LfPartyId = LfPartyId.assertFromString("david")

  "Correct view confirmation parameters" when {
    "adding a submitting admin party" should {
      "correctly update informees and thresholds" in {
        val oldInformees = Set(alice, bob, charlie)
        val oldThreshold = NonNegativeInt.tryCreate(2)
        val oldQuorum =
          Seq(
            Quorum(
              Map(
                bob -> PositiveInt.one,
                charlie -> PositiveInt.one,
              ),
              oldThreshold,
            )
          )
        val oldViewConfirmationParameters =
          ViewConfirmationParameters.tryCreate(oldInformees, oldQuorum)

        withSubmittingAdminParty(None)(oldViewConfirmationParameters) shouldBe
          oldViewConfirmationParameters

        withSubmittingAdminParty(Some(alice))(oldViewConfirmationParameters) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformees,
            oldQuorum :+
              Quorum(
                Map(alice -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )

        withSubmittingAdminParty(Some(bob))(oldViewConfirmationParameters) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformees,
            oldQuorum :+
              Quorum(
                Map(bob -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )

        withSubmittingAdminParty(Some(charlie))(oldViewConfirmationParameters) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformees,
            oldQuorum :+
              Quorum(
                Map(charlie -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )

        withSubmittingAdminParty(Some(david))(oldViewConfirmationParameters) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformees + david,
            oldQuorum :+
              Quorum(
                Map(david -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )
      }
    }

    "multiple quorums" in {
      val informees = Set(alice, bob, charlie)
      val quorums = Seq(
        Quorum(
          Map(
            alice -> PositiveInt.one
          ),
          NonNegativeInt.one,
        ),
        Quorum(
          Map(
            bob -> PositiveInt.one,
            charlie -> PositiveInt.one,
          ),
          NonNegativeInt.tryCreate(2),
        ),
      )

      val oldViewConfirmationParameters =
        ViewConfirmationParameters.tryCreate(informees, quorums)

      withSubmittingAdminParty(Some(david))(
        oldViewConfirmationParameters
      ) shouldBe {
        ViewConfirmationParameters.tryCreate(
          informees + david,
          quorums :+
            Quorum(
              Map(david -> PositiveInt.one),
              NonNegativeInt.one,
            ),
        )
      }
    }

    "no superfluous quorum is added" in {
      val informees = Set(alice, bob, charlie)
      val QuorumMultiple =
        Seq(
          Quorum(
            Map(
              alice -> PositiveInt.one
            ),
            NonNegativeInt.one,
          ),
          Quorum(
            Map(
              alice -> PositiveInt.one,
              bob -> PositiveInt.one,
              charlie -> PositiveInt.one,
            ),
            NonNegativeInt.tryCreate(3),
          ),
          Quorum(
            Map(
              bob -> PositiveInt.one
            ),
            NonNegativeInt.one,
          ),
        )
      val viewConfirmationParametersMultiple =
        ViewConfirmationParameters.tryCreate(informees, QuorumMultiple)

      withSubmittingAdminParty(Some(alice))(
        viewConfirmationParametersMultiple
      ) shouldBe viewConfirmationParametersMultiple

    }
  }
}
