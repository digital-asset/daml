// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.DefaultTestIdentities.{mediatorId, sequencerId}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
import com.digitalasset.canton.topology.store.TopologyTransactionRejection.InvalidTopologyMapping
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Observation,
  Submission,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ValidatingTopologyMappingChecksTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private lazy val factory = new TestingOwnerWithKeys(
    DefaultTestIdentities.mediatorId,
    loggerFactory,
    initEc = parallelExecutionContext,
  )

  def mk() = {
    val store = new InMemoryTopologyStore(AuthorizedStore, loggerFactory, timeouts)
    val check = new ValidatingTopologyMappingChecks(store, loggerFactory)
    (check, store)
  }

  "TopologyMappingChecks" when {
    import DefaultTestIdentities.{domainId, participant1, participant2, participant3, party1}
    import factory.TestingTransactions.*

    def checkTransaction(
        checks: TopologyMappingChecks,
        toValidate: GenericSignedTopologyTransaction,
        inStore: Option[GenericSignedTopologyTransaction] = None,
    ): Either[TopologyTransactionRejection, Unit] =
      checks.checkTransaction(EffectiveTime.MaxValue, toValidate, inStore).value.futureValue

    implicit def toHostingParticipant(
        participantToPermission: (ParticipantId, ParticipantPermission)
    ): HostingParticipant =
      HostingParticipant(participantToPermission._1, participantToPermission._2)

    "validating PartyToParticipant" should {

      "reject an invalid threshold" in {
        val (checks, _) = mk()

        val failureCases = Seq[(PositiveInt, Seq[HostingParticipant])](
          PositiveInt.two -> Seq(participant1 -> Observation, participant2 -> Confirmation),
          PositiveInt.two -> Seq(participant1 -> Observation, participant2 -> Submission),
          PositiveInt.two -> Seq(participant1 -> Submission),
          PositiveInt.one -> Seq(participant1 -> Observation),
        )

        failureCases.foreach { case (threshold, participants) =>
          val ptp = factory.mkAdd(
            PartyToParticipant(
              party1,
              None,
              threshold,
              participants,
              groupAddressing = false,
            )
          )

          checkTransaction(checks, ptp) should matchPattern {
            case Left(TopologyTransactionRejection.ThresholdTooHigh(`threshold`.value, _)) =>
          }
        }
      }

      "reject when participants don't have a DTC" in {
        val (checks, store) = mk()
        addToStore(store, p2_dtc)

        val failureCases = Seq(Seq(participant1), Seq(participant1, participant2))

        failureCases.foreach { participants =>
          val ptp = factory.mkAdd(
            PartyToParticipant(
              party1,
              None,
              PositiveInt.one,
              participants.map[HostingParticipant](_ -> Submission),
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.UnknownMembers(Seq(participant1))
          )
        }
      }

      "reject when participants don't have a valid encryption or signing key" in {
        val (checks, store) = mk()
        val p2MissingEncKey = factory.mkAdd(
          OwnerToKeyMapping(participant2, None, NonEmpty(Seq, factory.SigningKeys.key1))
        )
        val p3MissingSigningKey = factory.mkAdd(
          OwnerToKeyMapping(participant3, None, NonEmpty(Seq, factory.EncryptionKeys.key1))
        )

        addToStore(store, p1_dtc, p2_dtc, p3_dtc, p2MissingEncKey, p3MissingSigningKey)

        val missingKeyCases = Seq(participant1, participant2, participant3)

        missingKeyCases.foreach { participant =>
          val ptp = factory.mkAdd(
            PartyToParticipant(
              party1,
              None,
              PositiveInt.one,
              Seq(participant -> Submission),
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Left(
            TopologyTransactionRejection.InsufficientKeys(Seq(participant))
          )
        }
      }

      "report no errors for valid mappings" in {
        val (checks, store) = mk()
        addToStore(store, p1_otk, p1_dtc, p2_otk, p2_dtc, p3_otk, p3_dtc)

        val validCases = Seq[(PositiveInt, Seq[HostingParticipant])](
          PositiveInt.one -> Seq(participant1 -> Confirmation),
          PositiveInt.one -> Seq(participant1 -> Submission),
          PositiveInt.one -> Seq(participant1 -> Observation, participant2 -> Confirmation),
          PositiveInt.two -> Seq(participant1 -> Confirmation, participant2 -> Submission),
          PositiveInt.two -> Seq(
            participant1 -> Observation,
            participant2 -> Submission,
            participant3 -> Submission,
          ),
        )

        validCases.foreach { case (threshold, participants) =>
          val ptp = factory.mkAdd(
            PartyToParticipant(
              party1,
              None,
              threshold,
              participants,
              groupAddressing = false,
            )
          )
          checkTransaction(checks, ptp) shouldBe Right(())
        }
      }

    }

    "validating DomainTrustCertificate" should {
      "reject a removal when the participant still hosts a party" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          PartyToParticipant(
            party1,
            None,
            PositiveInt.one,
            Seq(participant1 -> Submission),
            groupAddressing = false,
          )
        )
        addToStore(
          store,
          ptp,
        )
        val prior = factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

        val dtc =
          factory.mkRemove(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

        checkTransaction(checks, dtc, Some(prior)) shouldBe Left(
          TopologyTransactionRejection.ParticipantStillHostsParties(participant1, Seq(party1))
        )

      }

      "reject the addition if the domain is locked" in {
        Seq(OnboardingRestriction.RestrictedLocked, OnboardingRestriction.UnrestrictedLocked)
          .foreach { restriction =>
            val (checks, store) = mk()
            val ptp = factory.mkAdd(
              DomainParametersState(
                domainId,
                DynamicDomainParameters
                  .defaultValues(testedProtocolVersion)
                  .tryUpdate(onboardingRestriction = restriction),
              )
            )
            addToStore(store, ptp)

            val dtc =
              factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty))

            checkTransaction(checks, dtc) shouldBe Left(
              TopologyTransactionRejection.OnboardingRestrictionInPlace(
                participant1,
                restriction,
                None,
              )
            )
          }
      }

      "reject the addition if the domain is restricted" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          DomainParametersState(
            domainId,
            DynamicDomainParameters
              .defaultValues(testedProtocolVersion)
              .tryUpdate(onboardingRestriction = OnboardingRestriction.RestrictedOpen),
          )
        )
        addToStore(
          store,
          ptp,
          factory.mkAdd(
            ParticipantDomainPermission(
              domainId,
              participant1,
              ParticipantPermission.Submission,
              None,
              None,
            )
          ),
        )

        // participant2 does not have permission from the domain to join
        checkTransaction(
          checks,
          factory.mkAdd(DomainTrustCertificate(participant2, domainId, false, Seq.empty)),
        ) shouldBe Left(
          TopologyTransactionRejection.OnboardingRestrictionInPlace(
            participant2,
            OnboardingRestriction.RestrictedOpen,
            None,
          )
        )

        // participant1 has been permissioned by the domain
        checkTransaction(
          checks,
          factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty)),
          None,
        ) shouldBe Right(())
      }
    }

    "validating OwnerToKeyMapping" should {
      "report no errors for valid mappings" in {
        val (checks, _) = mk()
        val okm_sequencer = factory.mkAddMultiKey(
          OwnerToKeyMapping(sequencerId, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediator = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participant = factory.mkAddMultiKey(
          OwnerToKeyMapping(
            participant1,
            None,
            NonEmpty(Seq, factory.EncryptionKeys.key1, factory.SigningKeys.key1),
          ),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        checkTransaction(checks, okm_sequencer) shouldBe Right(())
        checkTransaction(checks, okm_mediator) shouldBe Right(())
        checkTransaction(checks, okm_participant) shouldBe Right(())
      }
      "reject minimum key violations" in {
        val (checks, _) = mk()
        val okm_sequencerNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(sequencerId, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_mediatorNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(mediatorId, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoSigningKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, None, NonEmpty(Seq, factory.EncryptionKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )
        val okm_participantNoEncryptionKey = factory.mkAddMultiKey(
          OwnerToKeyMapping(participant1, None, NonEmpty(Seq, factory.SigningKeys.key1)),
          NonEmpty(Set, factory.SigningKeys.key1),
        )

        Seq(okm_sequencerNoSigningKey, okm_mediatorNoSigningKey, okm_participantNoSigningKey)
          .foreach(tx =>
            checkTransaction(checks, tx) shouldBe Left(
              InvalidTopologyMapping(
                "OwnerToKeyMapping must contain at least 1 signing key."
              )
            )
          )
        checkTransaction(checks, okm_participantNoEncryptionKey) shouldBe Left(
          InvalidTopologyMapping(
            "OwnerToKeyMapping for participants must contain at least 1 encryption key."
          )
        )
      }
    }
  }

  private def addToStore(
      store: TopologyStore[AuthorizedStore],
      transactions: GenericSignedTopologyTransaction*
  ): Unit = {
    store
      .bootstrap(
        StoredTopologyTransactions(
          transactions.map(tx =>
            StoredTopologyTransaction(SequencedTime.MinValue, EffectiveTime.MinValue, None, tx)
          )
        )
      )
      .futureValue
  }

}
