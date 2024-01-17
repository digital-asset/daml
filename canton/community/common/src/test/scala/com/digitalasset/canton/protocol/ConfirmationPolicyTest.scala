// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.value.Value
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{ConfirmingParty, PlainInformee}
import com.digitalasset.canton.protocol.ConfirmationPolicy.{Signatory, Vip}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  signatoryParticipant,
  submitterParticipant,
  templateId,
}
import com.digitalasset.canton.protocol.LfGlobalKeyWithMaintainers
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class ConfirmationPolicyTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private lazy val gen = new ExampleTransactionFactory()(confirmationPolicy = Vip)

  private lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  private lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")
  private lazy val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  private lazy val david: LfPartyId = LfPartyId.assertFromString("david")

  "Choice of a confirmation policy" when {
    "all views have at least one Vip participant" should {
      "favor the VIP policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.activeParticipantsOf(any[LfPartyId]))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Vip))
            )
          )
        val policies = gen.standardHappyCases
          .map(_.versionedUnsuffixedTransaction)
          .map(ConfirmationPolicy.choose(_, topologySnapshot).futureValue)
        assert(policies.forall(_.headOption === Some(Vip)))
      }
    }

    "some views have no VIP participant" should {
      "fall back to Signatory policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.activeParticipantsOf(any[LfPartyId]))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        val policies = gen.standardHappyCases
          .map(_.versionedUnsuffixedTransaction)
          .filter(
            _.nodes.nonEmpty
          ) // TODO (M12, i1046) handling of empty transaction remains a bit murky
          .map(ConfirmationPolicy.choose(_, topologySnapshot).futureValue)
        assert(policies.forall(_.headOption === Some(Signatory)))
      }
    }

    "a view's VIPs are not stakeholders" should {
      "fall back to Signatory policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.submitter)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Vip))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.signatory)))
          .thenReturn(
            Future.successful(
              Map(signatoryParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.observer)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        val policies = ConfirmationPolicy
          .choose(
            gen
              .SingleExerciseWithNonstakeholderActor(ExampleTransactionFactory.lfHash(0))
              .versionedUnsuffixedTransaction,
            topologySnapshot,
          )
          .futureValue
        assert(policies == Seq(Signatory))
      }
    }

    val txCreateWithKey = gen
      .SingleCreate(
        seed = gen.deriveNodeSeed(0),
        signatories = Set(ExampleTransactionFactory.signatory),
        observers = Set(ExampleTransactionFactory.submitter, ExampleTransactionFactory.observer),
        key = Some(
          LfGlobalKeyWithMaintainers.assertBuild(
            templateId,
            Value.ValueUnit,
            Set(ExampleTransactionFactory.signatory),
            shared = true,
          )
        ),
      )
      .versionedUnsuffixedTransaction

    "a view's VIPs are not key maintainers" should {
      "fall back to Signatory policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.submitter)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Vip))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.signatory)))
          .thenReturn(
            Future.successful(
              Map(signatoryParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.observer)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        val policies = ConfirmationPolicy.choose(txCreateWithKey, topologySnapshot).futureValue
        assert(policies == Seq(Signatory))
      }
    }

    "only some VIPs of a view are key maintainers" should {
      "favor the VIP policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.submitter)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Vip))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.signatory)))
          .thenReturn(
            Future.successful(
              Map(signatoryParticipant -> ParticipantAttributes(Submission, TrustLevel.Vip))
            )
          )
        when(topologySnapshot.activeParticipantsOf(eqTo(ExampleTransactionFactory.observer)))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )
        val policies = ConfirmationPolicy.choose(txCreateWithKey, topologySnapshot).futureValue
        assert(policies == Seq(Vip, Signatory))
      }
    }
  }

  "The signatory policy" when {
    "adding a submitting admin party" should {
      "correctly update informees and thresholds" in {
        val oldInformees = Set(
          PlainInformee(alice),
          ConfirmingParty(bob, PositiveInt.one, TrustLevel.Ordinary),
          ConfirmingParty(charlie, PositiveInt.one, TrustLevel.Vip),
        )
        val oldThreshold = NonNegativeInt.tryCreate(2)

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(None)(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(alice))(oldInformees, oldThreshold) shouldBe
          Set(
            ConfirmingParty(alice, PositiveInt.one, TrustLevel.Ordinary),
            ConfirmingParty(bob, PositiveInt.one, TrustLevel.Ordinary),
            ConfirmingParty(charlie, PositiveInt.one, TrustLevel.Vip),
          ) -> NonNegativeInt.tryCreate(3)

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(bob))(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(charlie))(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Signatory
          .withSubmittingAdminParty(Some(david))(oldInformees, oldThreshold) shouldBe
          oldInformees + ConfirmingParty(david, PositiveInt.one, TrustLevel.Ordinary) ->
          NonNegativeInt.tryCreate(3)
      }
    }
  }

  "The VIP policy" when {
    "adding a submitting admin party" should {
      "correctly update informees and thresholds" in {
        val oldInformees = Set(
          PlainInformee(alice),
          ConfirmingParty(bob, PositiveInt.one, TrustLevel.Vip),
          ConfirmingParty(charlie, PositiveInt.one, TrustLevel.Vip),
        )
        val oldThreshold = NonNegativeInt.one

        ConfirmationPolicy.Vip
          .withSubmittingAdminParty(None)(oldInformees, oldThreshold) shouldBe
          oldInformees -> oldThreshold

        ConfirmationPolicy.Vip
          .withSubmittingAdminParty(Some(alice))(oldInformees, oldThreshold) shouldBe
          Set(
            ConfirmingParty(alice, PositiveInt.tryCreate(3), TrustLevel.Ordinary),
            ConfirmingParty(bob, PositiveInt.one, TrustLevel.Vip),
            ConfirmingParty(charlie, PositiveInt.one, TrustLevel.Vip),
          ) -> NonNegativeInt.tryCreate(4)

        ConfirmationPolicy.Vip
          .withSubmittingAdminParty(Some(bob))(oldInformees, oldThreshold) shouldBe
          Set(
            PlainInformee(alice),
            ConfirmingParty(bob, PositiveInt.tryCreate(4), TrustLevel.Vip),
            ConfirmingParty(charlie, PositiveInt.one, TrustLevel.Vip),
          ) -> NonNegativeInt.tryCreate(4)

        ConfirmationPolicy.Vip
          .withSubmittingAdminParty(Some(david))(oldInformees, oldThreshold) shouldBe
          oldInformees + ConfirmingParty(david, PositiveInt.tryCreate(3), TrustLevel.Ordinary) ->
          NonNegativeInt.tryCreate(4)
      }
    }
  }
}
