// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{PublicKey, SigningKeyUsage}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestSynchronizerParameters
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.google.protobuf.ByteString
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionTest
    extends AnyWordSpec
    with BaseTest
    with HasCryptographicEvidenceTest
    with FailOnShutdown {

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
  private val publicKey =
    PublicKey
      .getLatestKey(
        crypto.currentSnapshotApproximation.ipsSnapshot
          .signingKeys(sequencerId, SigningKeyUsage.All)
          .futureValueUS
      )
      .getOrElse(sys.error("no keys"))
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
        mk(NamespaceDelegation.tryCreate(uid.namespace, publicKey, isRootDelegation = true))
      val nsd2 =
        mk(NamespaceDelegation.tryCreate(uid2.namespace, publicKey, isRootDelegation = false))

      runTest(nsd, nsd2)

    }

    "identifier delegations" should {
      val id1 = mk(IdentifierDelegation(uid, publicKey))
      val id2 = mk(IdentifierDelegation(uid2, publicKey))
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

}
