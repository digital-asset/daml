// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.{
  Hash,
  HashOps,
  SignatureCheckError,
  SyncCryptoError,
  TestHash,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{DomainParameters, DynamicDomainParameters}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.transaction.{
  ParticipantAttributes,
  ParticipantPermission,
  TrustLevel,
}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class TestingIdentityFactoryTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  import DefaultTestIdentities.*

  private def getMyHash(hashOps: HashOps, message: String = "dummySignature"): Hash =
    hashOps.build(TestHash.testHashPurpose).addWithoutLengthPrefix(message).finish()
  def await(eitherT: EitherT[Future, SignatureCheckError, Unit]) = eitherT.value.futureValue

  private def increaseParticipantResponseTimeout(old: DynamicDomainParameters) =
    old.tryUpdate(participantResponseTimeout =
      old.participantResponseTimeout + NonNegativeFiniteDuration.tryOfSeconds(1)
    )

  private val domainParameters1 = DomainParameters.WithValidity(
    CantonTimestamp.Epoch,
    Some(CantonTimestamp.ofEpochSecond(10)),
    increaseParticipantResponseTimeout(defaultDynamicDomainParameters),
  )

  private val domainParameters2 = DomainParameters.WithValidity(
    CantonTimestamp.ofEpochSecond(10),
    None,
    increaseParticipantResponseTimeout(domainParameters1.parameter),
  )

  val domainParameters = List(domainParameters1, domainParameters2)

  "testing topology" when {

    def compare(setup: TestingIdentityFactory): Unit = {
      val p1 = setup.forOwnerAndDomain(participant1)
      val p2 = setup.forOwnerAndDomain(participant2)
      val hash = getMyHash(p1.pureCrypto)
      val hash2 = getMyHash(p1.pureCrypto, "somethingElse")

      val signature =
        Await
          .result(p1.currentSnapshotApproximation.sign(hash).value, 10.seconds)
          .valueOr(err => fail(s"Failed to sign: $err"))

      "signature of participant1 is verifiable by participant1" in {
        await(
          p1.currentSnapshotApproximation.verifySignature(hash, participant1, signature)
        ) shouldBe Right(())
      }
      "signature of participant1 is verifiable by participant2" in {
        await(
          p2.currentSnapshotApproximation.verifySignature(hash, participant1, signature)
        ) shouldBe Right(())
      }
      "signature verification fails for wrong key owner" in {
        await(
          p1.currentSnapshotApproximation.verifySignature(hash, participant2, signature)
        ).left.value shouldBe a[SignatureCheckError]
      }
      "signature fails for invalid hash" in {
        await(
          p1.currentSnapshotApproximation.verifySignature(hash2, participant1, signature)
        ).left.value shouldBe a[SignatureCheckError]
        await(
          p1.currentSnapshotApproximation.verifySignature(hash2, participant2, signature)
        ).left.value shouldBe a[SignatureCheckError]
      }
      "participant1 is active" in {
        Seq(p1, p2).foreach(
          _.currentSnapshotApproximation.ipsSnapshot
            .participants()
            .futureValue
            .find(_._1 == participant1)
            .map(_._2) shouldBe Some(ParticipantPermission.Confirmation)
        )
      }
      "party1 is active" in {
        p1.currentSnapshotApproximation.ipsSnapshot
          .activeParticipantsOf(party1.toLf)
          .futureValue shouldBe (Map(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation, TrustLevel.Vip)
        ))
      }
      "participant2 can't sign messages without appropriate keys" in {
        Await
          .result(p2.currentSnapshotApproximation.sign(hash).value, 10.seconds)
          .left
          .value shouldBe a[SyncCryptoError]
      }

      def checkDomainKeys(did: UniqueIdentifier, expectedLength: Int): Unit = {
        Seq[KeyOwner](MediatorId(did), DomainTopologyManagerId(did), SequencerId(did)).foreach(
          member =>
            p1.currentSnapshotApproximation.ipsSnapshot
              .signingKeys(member)
              .futureValue should have length (expectedLength.toLong)
        )
      }

      "domain entities have keys" in {
        val did = p1.currentSnapshotApproximation.domainId.unwrap
        checkDomainKeys(did, 1)
      }
      "invalid domain entities don't have keys" in {
        val did = participant2.uid
        require(did != DefaultTestIdentities.domainId.unwrap)
        checkDomainKeys(did, 0)
      }

      "serve domain parameters corresponding to correct timestamp" in {
        def getParameters(ts: CantonTimestamp): DynamicDomainParameters =
          p1.ips
            .awaitSnapshot(ts)
            .flatMap(_.findDynamicDomainParametersOrDefault(testedProtocolVersion))
            .futureValue

        val transitionTs = domainParameters1.validUntil.value

        getParameters(CantonTimestamp.Epoch) shouldBe defaultDynamicDomainParameters
        getParameters(transitionTs.minusMillis(1)) shouldBe domainParameters1.parameter

        getParameters(transitionTs) shouldBe domainParameters1.parameter // validFrom is exclusive

        getParameters(transitionTs.plusMillis(1)) shouldBe domainParameters2.parameter
      }

    }

    "initialised directly" should {
      val topology = Map(
        party1.toLf -> Map(
          participant1 -> ParticipantPermission.Confirmation
        )
      )
      val setup = TestingTopology(
        topology = topology,
        domainParameters = domainParameters,
        participants = Map(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation, TrustLevel.Vip)
        ),
      ).build()
      compare(setup)
      // extend with admin parties should give participant2 a signing key
      val crypto2 = TestingTopology(topology = topology, domainParameters = domainParameters)
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation, TrustLevel.Vip),
          participant2 -> ParticipantAttributes(
            ParticipantPermission.Submission,
            TrustLevel.Ordinary,
          ),
        )
        .build()
      val p1 = crypto2.forOwnerAndDomain(participant1)
      val p2 = crypto2.forOwnerAndDomain(participant2)

      "extending with admin parties works" in {
        def check(p: ParticipantId) =
          p1.currentSnapshotApproximation.ipsSnapshot
            .activeParticipantsOf(p.adminParty.toLf)
            .futureValue
            .keys shouldBe Set(p)
        check(participant1)
        check(participant2)

      }

      val hash = getMyHash(p2.currentSnapshotApproximation.pureCrypto)

      val signature =
        Await
          .result(p2.currentSnapshotApproximation.sign(hash).value, 10.seconds)
          .valueOr(err => fail(s"Failed to sign: $err"))

      "participant2 signatures are valid" in {
        await(
          p2.currentSnapshotApproximation.verifySignature(hash, participant2, signature)
        ) shouldBe Right(())
        await(
          p1.currentSnapshotApproximation.verifySignature(hash, participant1, signature)
        ).left.value shouldBe a[SignatureCheckError]
      }

    }

    "using reverse topology" should {
      val setup = TestingTopology(domainParameters = domainParameters)
        .withReversedTopology(
          Map(participant1 -> Map(party1.toLf -> ParticipantPermission.Confirmation))
        )
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation, TrustLevel.Vip)
        )
        .build()
      compare(setup)

      "preserve topology and permissions" in {
        val syncCryptoApi =
          TestingTopology()
            .withReversedTopology(
              Map(
                participant1 -> Map(
                  party1.toLf -> ParticipantPermission.Observation,
                  party2.toLf -> ParticipantPermission.Confirmation,
                ),
                participant2 -> Map(party1.toLf -> ParticipantPermission.Submission),
              )
            )
            .build()
            .forOwnerAndDomain(participant1)
            .currentSnapshotApproximation
        def ol(permission: ParticipantPermission) =
          ParticipantAttributes(permission, TrustLevel.Ordinary)
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party1.toLf).futureValue shouldBe Map(
          participant1 -> ol(ParticipantPermission.Observation),
          participant2 -> ol(ParticipantPermission.Submission),
        )
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party3.toLf).futureValue shouldBe Map()
        syncCryptoApi.ipsSnapshot.activeParticipantsOf(party2.toLf).futureValue shouldBe Map(
          participant1 -> ol(ParticipantPermission.Confirmation)
        )
      }
    }

    "withTopology" should {
      val setup = TestingTopology(domainParameters = domainParameters)
        .withTopology(
          Map(party1.toLf -> participant1),
          ParticipantPermission.Confirmation,
        )
        .withParticipants(
          participant1 -> ParticipantAttributes(ParticipantPermission.Confirmation, TrustLevel.Vip)
        )
        .build()
      compare(setup)
    }

  }
}
