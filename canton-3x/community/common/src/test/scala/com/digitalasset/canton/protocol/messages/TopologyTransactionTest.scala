// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.BaseTest
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

  private def mk[T <: TopologyStateUpdateMapping](
      mapping: T
  ): TopologyStateUpdate[TopologyChangeOp.Add] =
    TopologyStateUpdate.createAdd(mapping, testedProtocolVersion)

  private val deserialize: ByteString => TopologyTransaction[TopologyChangeOp] =
    bytes =>
      TopologyTransaction.fromByteString(bytes) match {
        case Left(err) => throw new TestFailedException(err.toString, 0)
        case Right(msg) => msg
      }

  private def runTest(
      t1: TopologyTransaction[TopologyChangeOp],
      t2: TopologyTransaction[TopologyChangeOp],
  ): Unit = {
    behave like hasCryptographicEvidenceSerialization(t1, t2)
    behave like hasCryptographicEvidenceDeserialization(t1, t1.getCryptographicEvidence)(
      deserialize
    )
  }

  "domain topology transactions" when {

    "namespace mappings" should {

      val nsd = mk(NamespaceDelegation(uid.namespace, publicKey, true))
      val nsd2 = mk(NamespaceDelegation(uid2.namespace, publicKey, false))

      runTest(nsd, nsd2)

    }

    "identifier delegations" should {
      val id1 = mk(IdentifierDelegation(uid, publicKey))
      val id2 = mk(IdentifierDelegation(uid2, publicKey))
      runTest(id1, id2)
    }

    "key to owner mappings" should {
      val k1 = mk(OwnerToKeyMapping(managerId, publicKey))
      val k2 = mk(OwnerToKeyMapping(managerId, publicKey))
      runTest(k1, k2)
    }

    "party to participant" should {
      val p1 =
        mk(
          PartyToParticipant(
            RequestSide.From,
            PartyId(uid),
            ParticipantId(uid2),
            ParticipantPermission.Observation,
          )
        )
      val p2 =
        mk(
          PartyToParticipant(
            RequestSide.To,
            PartyId(uid2),
            ParticipantId(uid),
            ParticipantPermission.Observation,
          )
        )
      runTest(p1, p2)
    }

    "participant state" should {
      val ps1 = mk(
        ParticipantState(
          RequestSide.From,
          domainId,
          ParticipantId(uid),
          ParticipantPermission.Submission,
          TrustLevel.Vip,
        )
      )
      val ps2 = mk(
        ParticipantState(
          RequestSide.To,
          domainId,
          ParticipantId(uid2),
          ParticipantPermission.Confirmation,
          TrustLevel.Ordinary,
        )
      )

      runTest(ps1, ps2)

    }

    "domain parameters change" should {
      val dmp1 = DomainGovernanceTransaction(
        DomainParametersChange(DomainId(uid), defaultDynamicDomainParameters),
        testedProtocolVersion,
      )
      val dmp2 = DomainGovernanceTransaction(
        DomainParametersChange(DomainId(uid), defaultDynamicDomainParameters),
        testedProtocolVersion,
      )
      runTest(dmp1, dmp2)
    }

  }

}
