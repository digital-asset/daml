// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.*
import com.google.protobuf.ByteString
import org.scalatest.exceptions.TestFailedException
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {

  private val uid = DefaultTestIdentities.uid
  private val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da1::default1")
  private val managerId = DefaultTestIdentities.domainManager
  private val domainId = DefaultTestIdentities.domainId
  private val crypto =
    TestingTopology().build(loggerFactory).forOwnerAndDomain(managerId, domainId)
  private val publicKey =
    crypto.currentSnapshotApproximation.ipsSnapshot
      .signingKey(managerId)
      .futureValue
      .getOrElse(sys.error("no key"))
  private val defaultDynamicDomainParameters = TestDomainParameters.defaultDynamic

  private def mk[T <: TopologyMappingX](
      mapping: T
  ): TopologyTransactionX[TopologyChangeOpX.Replace, T] = {
    TopologyTransactionX(TopologyChangeOpX.Replace, PositiveInt.one, mapping, testedProtocolVersion)
  }

  private val deserialize: ByteString => TopologyTransactionX[TopologyChangeOpX, TopologyMappingX] =
    bytes =>
      TopologyTransactionX.fromByteString(
        testedProtocolVersionValidation
      )(
        bytes
      ) match {
        case Left(err) => throw new TestFailedException(err.toString, 0)
        case Right(msg) => msg
      }

  private def runTest(
      t1: TopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
      t2: TopologyTransactionX[TopologyChangeOpX, TopologyMappingX],
  ): Unit = {
    behave like hasCryptographicEvidenceSerialization(t1, t2)
    behave like hasCryptographicEvidenceDeserialization(t1, t1.getCryptographicEvidence)(
      deserialize
    )
  }

  "domain topology transactions" when {

    "namespace mappings" should {

      val nsd =
        mk(NamespaceDelegationX.tryCreate(uid.namespace, publicKey, isRootDelegation = true))
      val nsd2 =
        mk(NamespaceDelegationX.tryCreate(uid2.namespace, publicKey, isRootDelegation = false))

      runTest(nsd, nsd2)

    }

    "identifier delegations" should {
      val id1 = mk(IdentifierDelegationX(uid, publicKey))
      val id2 = mk(IdentifierDelegationX(uid2, publicKey))
      runTest(id1, id2)
    }

    "key to owner mappings" should {
      val k1 = mk(OwnerToKeyMappingX(managerId, None, NonEmpty(Seq, publicKey)))
      val k2 = mk(OwnerToKeyMappingX(managerId, None, NonEmpty(Seq, publicKey)))
      runTest(k1, k2)
    }

    "party to participant" should {
      val p1 =
        mk(
          PartyToParticipantX(
            PartyId(uid),
            None,
            PositiveInt.one,
            Seq(HostingParticipant(ParticipantId(uid2), ParticipantPermissionX.Observation)),
            groupAddressing = false,
          )
        )

      val p2 =
        mk(
          PartyToParticipantX(
            PartyId(uid),
            Some(domainId),
            PositiveInt.two,
            Seq(
              HostingParticipant(ParticipantId(uid2), ParticipantPermissionX.Observation),
              HostingParticipant(ParticipantId(uid), ParticipantPermissionX.Submission),
            ),
            groupAddressing = true,
          )
        )

      runTest(p1, p2)
    }

    "participant state" should {
      val ps1 = mk(
        ParticipantDomainPermissionX(
          domainId,
          ParticipantId(uid),
          ParticipantPermissionX.Submission,
          limits = None,
          loginAfter = None,
        )
      )
      val ps2 = mk(
        ParticipantDomainPermissionX(
          domainId,
          ParticipantId(uid),
          ParticipantPermissionX.Observation,
          limits = Some(ParticipantDomainLimits(13, 37, 42)),
          loginAfter = Some(CantonTimestamp.MinValue.plusSeconds(17)),
        )
      )

      runTest(ps1, ps2)

    }

    "domain parameters change" should {
      val dmp1 = mk(DomainParametersStateX(DomainId(uid), defaultDynamicDomainParameters))
      val dmp2 = mk(DomainParametersStateX(DomainId(uid), defaultDynamicDomainParameters))
      runTest(dmp1, dmp2)
    }

  }

}
