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
  CanSignSpecificMappings,
}
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
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

    def mkProtoTransaction(nsd: v30.NamespaceDelegation) = v30.TopologyTransaction(
      operation = v30.Enums.TopologyChangeOp.TOPOLOGY_CHANGE_OP_ADD_REPLACE,
      serial = 1,
      mapping = Some(
        v30.TopologyMapping(v30.TopologyMapping.Mapping.NamespaceDelegation(nsd))
      ),
    )

    "read legacy namespace delegations" in {
      // Test case: is_root_delegation=true, restriction=empty <=> CanSignAllMappings
      val rootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30),
        isRootDelegation = true,
        restriction = v30.NamespaceDelegation.Restriction.Empty,
      )
      val rootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllMappings,
        )
        .value
      // we don't need to check that inverse direction of serialization, because topology transactions are memoized,
      // therefore we only need to be able to serialize to the new format
      NamespaceDelegation.fromProtoV30(rootDelegationProto).value shouldBe rootFromScala

      val protoTx = mkProtoTransaction(rootDelegationProto)
      val scalaTxFromBytes = TopologyTransaction
        .fromTrustedByteString(
          UntypedVersionedMessage(
            UntypedVersionedMessage.Wrapper.Data(protoTx.toByteString),
            1,
          ).toByteString
        )
        .value
      scalaTxFromBytes.toByteString

      // Test case: is_root_delegation=false, restriction=empty <=> CanSignAllButNamespaceDelegations
      val nonRootDelegationProto = v30.NamespaceDelegation(
        uid.namespace.toProtoPrimitive,
        Some(publicKey.toProtoV30),
        isRootDelegation = false,
        restriction = v30.NamespaceDelegation.Restriction.Empty,
      )
      val nonRootFromScala = NamespaceDelegation
        .create(
          uid.namespace,
          publicKey,
          CanSignAllButNamespaceDelegations,
        )
        .value
      // we don't need to check that inverse direction of serialization, because topology transactions are memoized,
      // therefore we only need to be able to serialize to the new format
      NamespaceDelegation.fromProtoV30(nonRootDelegationProto).value shouldBe nonRootFromScala

      // Test case: is_root_delegation=false, restriction=non-empty <=> CanSignSpecificMappings
      Seq(
        v30.NamespaceDelegation.Restriction
          .CanSignAllMappings(v30.NamespaceDelegation.CanSignAllMappings()) -> CanSignAllMappings,
        v30.NamespaceDelegation.Restriction.CanSignAllButNamespaceDelegations(
          v30.NamespaceDelegation.CanSignAllButNamespaceDelegations()
        ) -> CanSignAllButNamespaceDelegations,
        v30.NamespaceDelegation.Restriction.CanSignSpecificMapings(
          v30.NamespaceDelegation.CanSignSpecificMappings(
            // all but UNSPECIFIED
            (v30.Enums.TopologyMappingCode.values.toSet - v30.Enums.TopologyMappingCode.TOPOLOGY_MAPPING_CODE_UNSPECIFIED).toSeq
              .sortBy(_.value)
          )
        ) -> CanSignSpecificMappings(NonEmpty.from(TopologyMapping.Code.all).value.toSet),
      ).foreach { case (protoRestriction, scalaRestriction) =>
        val restrictedDelegationProto = v30.NamespaceDelegation(
          uid.namespace.toProtoPrimitive,
          Some(publicKey.toProtoV30),
          isRootDelegation = false,
          restriction = protoRestriction,
        )
        val restrictedFromScala = NamespaceDelegation
          .create(
            uid.namespace,
            publicKey,
            scalaRestriction,
          )
          .value
        NamespaceDelegation
          .fromProtoV30(restrictedDelegationProto)
          .value shouldBe restrictedFromScala
        restrictedFromScala.toProto shouldBe restrictedDelegationProto
      }
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
