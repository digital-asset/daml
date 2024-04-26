// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, OnboardingRestriction}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.AuthorizedStore
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
    DefaultTestIdentities.mediatorIdX,
    loggerFactory,
    initEc = parallelExecutionContext,
  )

  def mk() = {
    val store = new InMemoryTopologyStore(AuthorizedStore, loggerFactory, timeouts)
    val check = new ValidatingTopologyMappingChecks(store, loggerFactory)
    (check, store)
  }

  "TopologyMappingXChecks" when {
    import DefaultTestIdentities.{participant1, participant2, participant3, party1, domainId}
    import factory.TestingTransactions.*

    implicit def toHostingParticipant(
        participantToPermission: (ParticipantId, ParticipantPermission)
    ): HostingParticipant =
      HostingParticipant(participantToPermission._1, participantToPermission._2)

    "validating PartyToParticipantX" should {

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
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue should matchPattern {
            case Left(
                  TopologyTransactionRejection.ThresholdTooHigh(`threshold`.value, _)
                ) =>
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
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Left(
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
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Left(
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
          val result = checks.checkTransaction(EffectiveTime.MaxValue, ptp, None)
          result.value.futureValue shouldBe Right(())
        }
      }

    }

    "validating DomainTrustCertificateX" should {
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

        val result = checks.checkTransaction(EffectiveTime.MaxValue, dtc, Some(prior))
        result.value.futureValue shouldBe Left(
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

            val result = checks.checkTransaction(EffectiveTime.MaxValue, dtc, None)
            result.value.futureValue shouldBe Left(
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

        val dtc =
          factory.mkAdd(DomainTrustCertificate(participant2, domainId, false, Seq.empty))

        val result1 = checks.checkTransaction(EffectiveTime.MaxValue, dtc, None)
        result1.value.futureValue shouldBe Left(
          TopologyTransactionRejection.OnboardingRestrictionInPlace(
            participant2,
            OnboardingRestriction.RestrictedOpen,
            None,
          )
        )

        val result2 = checks.checkTransaction(
          EffectiveTime.MaxValue,
          factory.mkAdd(DomainTrustCertificate(participant1, domainId, false, Seq.empty)),
          None,
        )
        result2.value.futureValue shouldBe Right(())

      }

    }

    "validating TrafficControlStateX" should {
      def trafficControlState(limit: Int): TrafficControlState =
        TrafficControlState
          .create(domainId, participant1, PositiveLong.tryCreate(limit.toLong))
          .getOrElse(sys.error("Error creating TrafficControlStateX"))

      val limit5 = factory.mkAdd(trafficControlState(5))
      val limit10 = factory.mkAdd(trafficControlState(10))
      val removal10 = factory.mkRemove(trafficControlState(10))

      "reject non monotonically increasing extra traffict limits" in {
        val (checks, _) = mk()

        val result =
          checks.checkTransaction(
            EffectiveTime.MaxValue,
            toValidate = limit5,
            inStore = Some(limit10),
          )
        result.value.futureValue shouldBe
          Left(
            TopologyTransactionRejection.ExtraTrafficLimitTooLow(
              participant1,
              PositiveLong.tryCreate(5),
              PositiveLong.tryCreate(10),
            )
          )

      }

      "report no errors for valid mappings" in {
        val (checks, _) = mk()

        def runSuccessfulCheck(
            toValidate: SignedTopologyTransaction[TopologyChangeOp, TrafficControlState],
            inStore: Option[SignedTopologyTransaction[TopologyChangeOp, TrafficControlState]],
        ) =
          checks
            .checkTransaction(EffectiveTime.MaxValue, toValidate, inStore)
            .value
            .futureValue shouldBe Right(())

        // first limit for member
        runSuccessfulCheck(limit10, None)

        // increase limit
        runSuccessfulCheck(limit10, Some(limit5))

        // same limit
        runSuccessfulCheck(limit5, Some(limit5))

        // reset monotonicity after removal
        runSuccessfulCheck(limit5, Some(removal10))

        // remove traffic control state for member
        runSuccessfulCheck(removal10, Some(limit10))
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
