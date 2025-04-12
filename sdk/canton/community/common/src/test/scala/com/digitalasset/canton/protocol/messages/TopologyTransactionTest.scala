// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.SigningKeyUsage
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.{TestSynchronizerParameters, v30}
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
}
import com.digitalasset.canton.{BaseTest, FailOnShutdown, HasExecutionContext, LfPackageId}
import com.google.protobuf.ByteString
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionTest
    extends AnyWordSpec
    with BaseTest
    with HasCryptographicEvidenceTest
    with FailOnShutdown
    with HasExecutionContext {

  private val uid = DefaultTestIdentities.uid
  private val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da1::default1")
  private val sequencerId = DefaultTestIdentities.daSequencerId
  private val synchronizerId = DefaultTestIdentities.synchronizerId
  private val crypto =
    TestingTopology(sequencerGroup =
      SequencerGroup(
        active = Seq(SequencerId(synchronizerId.uid)),
        passive = Seq.empty,
        threshold = PositiveInt.one,
      )
    ).build(loggerFactory).forOwnerAndSynchronizer(sequencerId, synchronizerId)
  // TODO(#25072): Create keys with a single usage and change the tests accordingly
  private val publicKey =
    crypto.crypto.privateCrypto
      .generateSigningKey(usage = SigningKeyUsage.All)
      .valueOrFail("create public key")
      .futureValueUS
  private val defaultDynamicSynchronizerParameters = TestSynchronizerParameters.defaultDynamic

  private def mk[T <: TopologyMapping](
      mapping: T
  ): TopologyTransaction[TopologyChangeOp.Replace, T] =
    TopologyTransaction(TopologyChangeOp.Replace, PositiveInt.one, mapping, testedProtocolVersion)

  private val deserialize: ByteString => TopologyTransaction[TopologyChangeOp, TopologyMapping] =
    bytes =>
      TopologyTransaction.fromByteString(testedProtocolVersionValidation, bytes) match {
        case Left(err) => throw new TestFailedException(err.toString, 0)
        case Right(msg) => msg
      }

  private def runTest(
      t1: TopologyTransaction[TopologyChangeOp, TopologyMapping],
      t2: TopologyTransaction[TopologyChangeOp, TopologyMapping],
  ): Unit = {
    behave like hasCryptographicEvidenceSerialization(t1, t2)
    behave like hasCryptographicEvidenceDeserialization(t1, t1.getCryptographicEvidence)(
      deserialize
    )
  }

  "synchronizer topology transactions" when {

    "namespace mappings" should {
      val nsd =
        mk(NamespaceDelegation.tryCreate(uid.namespace, publicKey, CanSignAllMappings))
      val nsd2 =
        mk(
          NamespaceDelegation.tryCreate(
            uid2.namespace,
            publicKey,
            CanSignAllButNamespaceDelegations,
          )
        )

      runTest(nsd, nsd2)
    }

    "namespace delegation restrictions are backwards compatible" in {
      val legacyRootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30),
        isRootDelegation = true,
        restrictedToMappings = Seq.empty,
      )
      val rootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllMappings,
        )
        .value
      NamespaceDelegation.fromProtoV30(legacyRootDelegationProto).value shouldBe rootFromScala
      rootFromScala.toProto shouldBe legacyRootDelegationProto

      val legacyNonRootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30),
        isRootDelegation = false,
        restrictedToMappings = Seq.empty,
      )
      val nonRootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllButNamespaceDelegations,
        )
        .value
      NamespaceDelegation.fromProtoV30(legacyNonRootDelegationProto).value shouldBe nonRootFromScala
      nonRootFromScala.toProto shouldBe legacyNonRootDelegationProto
    }

    "identifier delegations" should {
      val id1 = mk(IdentifierDelegation.tryCreate(uid, publicKey))
      val id2 = mk(IdentifierDelegation.tryCreate(uid2, publicKey))
      runTest(id1, id2)
    }

    "key to owner mappings" should {
      val k1 = mk(OwnerToKeyMapping(sequencerId, NonEmpty(Seq, publicKey)))
      val k2 = mk(OwnerToKeyMapping(sequencerId, NonEmpty(Seq, publicKey)))
      runTest(k1, k2)
    }

    "party to participant" should {
      val p1 =
        mk(
          PartyToParticipant.tryCreate(
            PartyId(uid),
            PositiveInt.one,
            Seq(HostingParticipant(ParticipantId(uid2), ParticipantPermission.Observation)),
          )
        )

      val p2 =
        mk(
          PartyToParticipant.tryCreate(
            PartyId(uid),
            PositiveInt.two,
            Seq(
              HostingParticipant(ParticipantId(uid2), ParticipantPermission.Confirmation),
              HostingParticipant(ParticipantId(uid), ParticipantPermission.Submission),
            ),
          )
        )

      runTest(p1, p2)
    }

    "participant state" should {
      val ps1 = mk(
        ParticipantSynchronizerPermission(
          synchronizerId,
          ParticipantId(uid),
          ParticipantPermission.Submission,
          limits = None,
          loginAfter = None,
        )
      )
      val ps2 = mk(
        ParticipantSynchronizerPermission(
          synchronizerId,
          ParticipantId(uid),
          ParticipantPermission.Observation,
          limits = Some(ParticipantSynchronizerLimits(NonNegativeInt.tryCreate(13))),
          loginAfter = Some(CantonTimestamp.MinValue.plusSeconds(17)),
        )
      )

      runTest(ps1, ps2)

    }

    "synchronizer parameters change" should {
      val dmp1 =
        mk(SynchronizerParametersState(SynchronizerId(uid), defaultDynamicSynchronizerParameters))
      val dmp2 =
        mk(SynchronizerParametersState(SynchronizerId(uid), defaultDynamicSynchronizerParameters))
      runTest(dmp1, dmp2)
    }

  }

  "authorized store topology transactions" when {
    "package vetting" should {
      "honor specified LET boundaries" in {
        val validFrom = CantonTimestamp.ofEpochSecond(20)
        val validUntil = CantonTimestamp.ofEpochSecond(30)
        val vp =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), Some(validFrom), Some(validUntil))
        assert(!vp.validAt(validFrom.immediatePredecessor), "before valid-from invalid")
        // see https://github.com/DACH-NY/canton-network-node/issues/18259 regarding valid-from inclusivity:
        assert(vp.validAt(validFrom), "valid-from must be inclusive")
        assert(vp.validAt(validFrom.immediateSuccessor), "between must be valid")
        assert(!vp.validAt(validUntil), "valid-until must be exclusive")
      }

      "honor open ended LET boundaries" in {
        val validFrom = CantonTimestamp.ofEpochSecond(20)
        val untilForever =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), Some(validFrom), None)
        assert(untilForever.validAt(CantonTimestamp.MaxValue), "valid until forever")

        val validUntil = CantonTimestamp.ofEpochSecond(20)
        val sinceForever =
          VettedPackage(LfPackageId.assertFromString("pkg-id"), None, Some(validUntil))
        assert(sinceForever.validAt(CantonTimestamp.MinValue), "valid since forever")
      }
    }
  }
}
