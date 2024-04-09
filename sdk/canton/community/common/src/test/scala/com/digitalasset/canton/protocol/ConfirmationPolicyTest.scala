// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.value.Value
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  ConfirmingParty,
  PlainInformee,
  Quorum,
  ViewConfirmationParameters,
}
import com.digitalasset.canton.protocol.ConfirmationPolicy.{Signatory, Vip}
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  signatoryParticipant,
  submitterParticipant,
  templateId,
}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{Observation, Submission}
import com.digitalasset.canton.topology.transaction.{ParticipantAttributes, TrustLevel}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTest,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAsyncWordSpec,
}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ConfirmationPolicyTest
    extends AsyncWordSpec
    with ProtocolVersionChecksAsyncWordSpec
    with BaseTest
    with HasExecutionContext {

  private lazy val gen = new ExampleTransactionFactory()(confirmationPolicy = Vip)

  private lazy val alice: LfPartyId = LfPartyId.assertFromString("alice")
  private lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")
  private lazy val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  private lazy val david: LfPartyId = LfPartyId.assertFromString("david")

  private def withSubmittingAdminPartySignatory(
      submittingAdminPartyO: Option[LfPartyId]
  )(viewConfirmationParameters: ViewConfirmationParameters) =
    if (testedProtocolVersion >= ProtocolVersion.v6)
      ConfirmationPolicy.Signatory.withSubmittingAdminPartyQuorum(submittingAdminPartyO)(
        viewConfirmationParameters
      )
    else
      ConfirmationPolicy.Signatory.withSubmittingAdminParty(submittingAdminPartyO)(
        viewConfirmationParameters
      )

  private def withSubmittingAdminPartyVip(
      submittingAdminPartyO: Option[LfPartyId]
  )(viewConfirmationParameters: ViewConfirmationParameters) =
    if (testedProtocolVersion >= ProtocolVersion.v6)
      ConfirmationPolicy.Vip.withSubmittingAdminPartyQuorum(submittingAdminPartyO)(
        viewConfirmationParameters
      )
    else
      ConfirmationPolicy.Vip.withSubmittingAdminParty(submittingAdminPartyO)(
        viewConfirmationParameters
      )

  "Choice of a confirmation policy" when {
    "nodes with a signatory without confirming participant" should {
      "fail to provide a valid confirming policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        val tx = gen
          .SingleExerciseWithNonstakeholderActor(ExampleTransactionFactory.lfHash(0))
          .versionedUnsuffixedTransaction

        when(topologySnapshot.activeParticipantsOf(any[LfPartyId]))
          .thenAnswer[LfPartyId] {
            case ExampleTransactionFactory.signatory =>
              Future.successful(
                // Give the signatory Observation permission, which shouldn't be enough to get a valid confirmation policy
                Map(signatoryParticipant -> ParticipantAttributes(Observation, TrustLevel.Ordinary))
              )
            case _ =>
              Future.successful(
                Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
              )
          }

        val policies = ConfirmationPolicy
          .choose(tx, topologySnapshot)
          .futureValue

        assert(policies == Seq.empty)
      }
    }

    "nodes without confirming parties" should {
      "fail to provide a valid confirming policy" in {
        val topologySnapshot = mock[TopologySnapshot]
        val tx = gen
          .SingleExerciseWithoutConfirmingParties(ExampleTransactionFactory.lfHash(0))
          .versionedUnsuffixedTransaction

        when(topologySnapshot.activeParticipantsOf(any[LfPartyId]))
          .thenReturn(
            Future.successful(
              Map(submitterParticipant -> ParticipantAttributes(Submission, TrustLevel.Ordinary))
            )
          )

        val policies = ConfirmationPolicy
          .choose(tx, topologySnapshot)
          .futureValue

        assert(policies == Seq.empty)
      }
    }

    "all views have at least one VIP participant" should {
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
            KeyPackageName.assertBuild(None, ExampleTransactionFactory.languageVersion),
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
        val oldInformeesId =
          oldInformees.map(informee => informee.party -> informee.requiredTrustLevel).toMap
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
          ViewConfirmationParameters.tryCreate(oldInformeesId, oldQuorum)

        withSubmittingAdminPartySignatory(None)(
          oldViewConfirmationParameters
        ) shouldBe
          oldViewConfirmationParameters

        withSubmittingAdminPartySignatory(Some(alice))(
          oldViewConfirmationParameters
        ) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformeesId,
            if (testedProtocolVersion <= ProtocolVersion.v5)
              Seq(
                Quorum(
                  Map(
                    alice -> PositiveInt.one,
                    bob -> PositiveInt.one,
                    charlie -> PositiveInt.one,
                  ),
                  NonNegativeInt.tryCreate(3),
                )
              )
            else
              oldQuorum :+
                Quorum(
                  Map(alice -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
          )

        withSubmittingAdminPartySignatory(Some(bob))(
          oldViewConfirmationParameters
        ) shouldBe {
          if (testedProtocolVersion <= ProtocolVersion.v5) oldViewConfirmationParameters
          else
            ViewConfirmationParameters.tryCreate(
              oldInformeesId,
              oldQuorum :+
                Quorum(
                  Map(bob -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
            )
        }

        withSubmittingAdminPartySignatory(Some(charlie))(
          oldViewConfirmationParameters
        ) shouldBe {
          if (testedProtocolVersion <= ProtocolVersion.v5) oldViewConfirmationParameters
          else
            ViewConfirmationParameters.tryCreate(
              oldInformeesId,
              oldQuorum :+
                Quorum(
                  Map(charlie -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
            )
        }

        withSubmittingAdminPartySignatory(Some(david))(
          oldViewConfirmationParameters
        ) shouldBe {
          if (testedProtocolVersion <= ProtocolVersion.v5)
            ViewConfirmationParameters.create(
              oldInformees + ConfirmingParty(david, PositiveInt.one, TrustLevel.Ordinary),
              NonNegativeInt.tryCreate(3),
            )
          else
            ViewConfirmationParameters.tryCreate(
              oldInformeesId + (david -> TrustLevel.Ordinary),
              oldQuorum :+
                Quorum(
                  Map(david -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
            )
        }
      }

      "multiple quorums" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val informees =
          Map(alice -> TrustLevel.Ordinary, bob -> TrustLevel.Ordinary, charlie -> TrustLevel.Vip)
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

        withSubmittingAdminPartySignatory(Some(david))(
          oldViewConfirmationParameters
        ) shouldBe {
          ViewConfirmationParameters.tryCreate(
            informees + (david -> TrustLevel.Ordinary),
            quorums :+
              Quorum(
                Map(david -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )
        }
      }

      "no unnecessary weight/threshold changes" onlyRunWithOrLowerThan ProtocolVersion.v5 in {
        val informees =
          Map(alice -> TrustLevel.Ordinary, bob -> TrustLevel.Ordinary, charlie -> TrustLevel.Vip)
        val QuorumSingle =
          Seq(
            Quorum(
              Map(
                alice -> PositiveInt.one
              ),
              NonNegativeInt.one,
            )
          )
        val viewConfirmationParametersSingle =
          ViewConfirmationParameters.tryCreate(informees, QuorumSingle)

        withSubmittingAdminPartySignatory(Some(alice))(
          viewConfirmationParametersSingle
        ) shouldBe viewConfirmationParametersSingle

      }

      "no superfluous quorum is added" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val informees =
          Map(alice -> TrustLevel.Ordinary, bob -> TrustLevel.Ordinary, charlie -> TrustLevel.Vip)
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

        withSubmittingAdminPartySignatory(Some(alice))(
          viewConfirmationParametersMultiple
        ) shouldBe viewConfirmationParametersMultiple

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
        val oldInformeesId =
          oldInformees.map(informee => informee.party -> informee.requiredTrustLevel).toMap
        val oldThreshold = NonNegativeInt.one
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
          ViewConfirmationParameters.tryCreate(oldInformeesId, oldQuorum)

        withSubmittingAdminPartyVip(None)(
          oldViewConfirmationParameters
        ) shouldBe
          oldViewConfirmationParameters

        withSubmittingAdminPartyVip(Some(alice))(
          oldViewConfirmationParameters
        ) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformeesId,
            if (testedProtocolVersion <= ProtocolVersion.v5)
              Seq(
                Quorum(
                  Map(
                    alice -> PositiveInt.tryCreate(3),
                    bob -> PositiveInt.one,
                    charlie -> PositiveInt.one,
                  ),
                  NonNegativeInt.tryCreate(4),
                )
              )
            else
              oldQuorum :+
                Quorum(
                  Map(alice -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
          )

        withSubmittingAdminPartyVip(Some(bob))(
          oldViewConfirmationParameters
        ) shouldBe
          ViewConfirmationParameters.tryCreate(
            oldInformeesId,
            if (testedProtocolVersion <= ProtocolVersion.v5)
              Seq(
                Quorum(
                  Map(
                    bob -> PositiveInt.tryCreate(4),
                    charlie -> PositiveInt.one,
                  ),
                  NonNegativeInt.tryCreate(4),
                )
              )
            else
              oldQuorum :+
                Quorum(
                  Map(bob -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
          )

        withSubmittingAdminPartyVip(Some(david))(
          oldViewConfirmationParameters
        ) shouldBe {
          if (testedProtocolVersion <= ProtocolVersion.v5)
            ViewConfirmationParameters.create(
              oldInformees + ConfirmingParty(david, PositiveInt.tryCreate(3), TrustLevel.Ordinary),
              NonNegativeInt.tryCreate(4),
            )
          else
            ViewConfirmationParameters.tryCreate(
              oldInformeesId + (david -> TrustLevel.Ordinary),
              oldQuorum :+
                Quorum(
                  Map(david -> PositiveInt.one),
                  NonNegativeInt.one,
                ),
            )
        }
      }

      "multiple quorums" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val informees =
          Map(alice -> TrustLevel.Vip, bob -> TrustLevel.Vip, charlie -> TrustLevel.Vip)
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

        withSubmittingAdminPartyVip(Some(david))(
          oldViewConfirmationParameters
        ) shouldBe {
          ViewConfirmationParameters.tryCreate(
            informees + (david -> TrustLevel.Ordinary),
            quorums :+
              Quorum(
                Map(david -> PositiveInt.one),
                NonNegativeInt.one,
              ),
          )
        }
      }

      "no unnecessary weight/threshold changes" onlyRunWithOrLowerThan ProtocolVersion.v5 in {
        val informees =
          Map(alice -> TrustLevel.Vip, bob -> TrustLevel.Vip, charlie -> TrustLevel.Vip)
        val QuorumSingle =
          Seq(
            Quorum(
              Map(
                alice -> PositiveInt.one
              ),
              NonNegativeInt.one,
            )
          )
        val viewConfirmationParametersSingle =
          ViewConfirmationParameters.tryCreate(informees, QuorumSingle)

        withSubmittingAdminPartyVip(Some(alice))(
          viewConfirmationParametersSingle
        ) shouldBe viewConfirmationParametersSingle
      }

      "no superfluous quorum is added" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val informees =
          Map(alice -> TrustLevel.Vip, bob -> TrustLevel.Vip, charlie -> TrustLevel.Vip)
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

        withSubmittingAdminPartyVip(Some(alice))(
          viewConfirmationParametersMultiple
        ) shouldBe viewConfirmationParametersMultiple
      }

    }
  }
}
