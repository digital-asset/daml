// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.checks.OptionalTopologyMappingChecks
import com.digitalasset.canton.topology.{DefaultTestIdentities, MediatorId, SequencerId}

import scala.annotation.nowarn

@nowarn("msg=match may not be exhaustive")
class OptionalTopologyMappingChecksTest
    extends BaseTopologyMappingChecksTest[OptionalTopologyMappingChecks] {

  override protected def mkChecks(
      store: TopologyStore[SynchronizerStore]
  ): OptionalTopologyMappingChecks =
    new OptionalTopologyMappingChecks(
      store,
      loggerFactory,
    )

  "OptionalTopologyMappingChecks" when {
    import DefaultTestIdentities.{synchronizerId, participant1, party1}
    import factory.TestingTransactions.*

    "validating SynchronizerTrustCertificate" should {
      "reject a removal when the participant still hosts a party" in {
        val (checks, store) = mk()
        val ptp = factory.mkAdd(
          PartyToParticipant.tryCreate(
            party1,
            PositiveInt.one,
            Seq(participant1 -> ParticipantPermission.Submission),
          )
        )
        addToStore(
          store,
          ptp,
        )
        val prior = factory.mkAdd(SynchronizerTrustCertificate(participant1, synchronizerId))

        val dtc =
          factory.mkRemove(SynchronizerTrustCertificate(participant1, synchronizerId))
        checkTransaction(checks, dtc, Some(prior)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping
            .ParticipantStillHostsParties(participant1, Seq(party1))
        )
      }
    }
    "validating MediatorSynchronizerState" should {

      "report MembersCannotRejoinSynchronizer for mediators that are being re-onboarded" in {
        val (checks, store) = mk()
        val (Seq(med1, med2, med3), transactions) = generateMemberIdentities(3, MediatorId(_))

        val Seq(group0, group1) = mkMediatorGroups(
          PositiveInt.one,
          NonNegativeInt.zero -> Seq(med1, med3),
          NonNegativeInt.one -> Seq(med2, med3),
        )

        addToStore(store, (transactions :+ group0 :+ group1)*)

        val Seq(group0RemoveMed1, group1RemoveMed2) = mkMediatorGroups(
          PositiveInt.two,
          NonNegativeInt.zero -> Seq(med3),
          NonNegativeInt.one -> Seq(med3),
        )

        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removeMapping = Map(
              group0.mapping.uniqueKey -> PositiveInt.one,
              group1.mapping.uniqueKey -> PositiveInt.one,
            ),
            removeTxs = Set.empty,
            additions = Seq(
              ValidatedTopologyTransaction(group0RemoveMed1),
              ValidatedTopologyTransaction(group1RemoveMed2),
            ),
          )
          .futureValueUS

        val Seq(med1RejoinsGroup0, med2RejoinsGroup0) = mkMediatorGroups(
          PositiveInt.three,
          // try joining the same group
          NonNegativeInt.zero -> Seq(med1, med3),
          // try joining another group
          NonNegativeInt.zero -> Seq(med2, med3),
        )

        checkTransaction(checks, med1RejoinsGroup0, Some(group0RemoveMed1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(med1))
        )

        checkTransaction(checks, med2RejoinsGroup0, Some(group0RemoveMed1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(med2))
        )
      }

    }

    "validating SequencerSynchronizerState" should {

      "report MembersCannotRejoinSynchronizer for sequencers that are being re-onboarded" in {
        val (checks, store) = mk()
        val (Seq(seq1, seq2), transactions) = generateMemberIdentities(2, SequencerId(_))

        val sds_S1_S2 = makeSynchronizerState(
          PositiveInt.one,
          seq1,
          seq2,
        )

        addToStore(store, (transactions :+ sds_S1_S2)*)

        val sds_S1 = makeSynchronizerState(PositiveInt.two, seq1)

        store
          .update(
            SequencedTime(ts1),
            EffectiveTime(ts1),
            removeMapping = Map(
              sds_S1.mapping.uniqueKey -> PositiveInt.one
            ),
            removeTxs = Set.empty,
            additions = Seq(
              ValidatedTopologyTransaction(sds_S1)
            ),
          )
          .futureValueUS

        val sds_S1_rejoining_S2 = makeSynchronizerState(PositiveInt.three, seq1, seq2)

        checkTransaction(checks, sds_S1_rejoining_S2, Some(sds_S1)) shouldBe Left(
          TopologyTransactionRejection.OptionalMapping.MembersCannotRejoinSynchronizer(Seq(seq2))
        )
      }

    }

  }

}
