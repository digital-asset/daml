// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.BaseTest.testedProtocolVersionValidation
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.protocol.TestDomainParameters
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, ProtoDeserializationError}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class TopologyTransactionTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {

  private val fingerprint = Fingerprint.tryCreate("default")
  private val fingerprint2 = Fingerprint.tryCreate("default")
  private val pubKey = SymbolicCrypto.signingPublicKey("key1")
  private val pubKey2 = SymbolicCrypto.signingPublicKey("key2")
  private val uid = UniqueIdentifier.tryFromProtoPrimitive("da::tluafed")
  private val uid2 = UniqueIdentifier.tryFromProtoPrimitive("da2::tluafed")
  private val defaultDynamicDomainParameters = TestDomainParameters.defaultDynamic

  def testConversion[Op <: TopologyChangeOp, M <: TopologyMapping](
      builder: (M, ProtocolVersion) => TopologyTransaction[Op],
      fromByteString: ByteString => ParsingResult[TopologyTransaction[Op]],
  )(
      mapping: M,
      mapping2: Option[M] = None,
      hint: String = "",
  ): Unit = {
    val transaction = builder(mapping, testedProtocolVersion)
    val transaction2 = builder(mapping2.getOrElse(mapping), testedProtocolVersion)
    val serialized = transaction.getCryptographicEvidence

    val deserializer = fromByteString andThen ({
      case Left(x) => fail(x.toString)
      case Right(x) => x
    })

    behave like hasCryptographicEvidenceSerialization(transaction, transaction2, hint)
    behave like memoizedNondeterministicDeserialization(transaction, serialized, hint)(
      deserializer
    )

  }

  "topology transaction serialization & deserialization is identity" when {
    "namespace delegation" should {
      val ns1 = NamespaceDelegation(Namespace(fingerprint), pubKey, true)
      val ns2 = NamespaceDelegation(Namespace(fingerprint2), pubKey2, true)
      testConversion(
        TopologyStateUpdate.createAdd,
        TopologyTransaction.fromByteString(
          testedProtocolVersionValidation
        ),
      )(
        ns1,
        Some(ns2),
      )
    }
    "identifier delegation" should {
      testConversion(
        TopologyStateUpdate.createAdd,
        TopologyTransaction.fromByteString(
          testedProtocolVersionValidation
        ),
      )(
        IdentifierDelegation(uid, pubKey),
        Some(IdentifierDelegation(uid2, pubKey2)),
      )
    }
    "owner to key mapping" should {
      val owners = Seq[KeyOwner](
        ParticipantId(uid),
        MediatorId(uid),
        SequencerId(uid),
        DomainTopologyManagerId(uid),
      )
      owners.foreach(owner =>
        testConversion(
          TopologyStateUpdate.createAdd,
          TopologyTransaction.fromByteString(
            testedProtocolVersionValidation
          ),
        )(
          OwnerToKeyMapping(owner, pubKey),
          Some(OwnerToKeyMapping(owner, pubKey2)),
          hint = " for " + owner.toString,
        )
      )
    }
    "party to participant" should {
      val sides = Seq[(RequestSide, ParticipantPermission)](
        (RequestSide.From, ParticipantPermission.Confirmation),
        (RequestSide.Both, ParticipantPermission.Submission),
        (RequestSide.To, ParticipantPermission.Observation),
      )
      sides.foreach { case (side, permission) =>
        testConversion(
          TopologyStateUpdate.createAdd,
          TopologyTransaction.fromByteString(
            testedProtocolVersionValidation
          ),
        )(
          PartyToParticipant(side, PartyId(uid), ParticipantId(uid2), permission),
          Some(PartyToParticipant(side, PartyId(uid2), ParticipantId(uid), permission)),
          hint = " for " + side.toString + " and " + permission.toString,
        )
      }
    }

    "domain parameters change" should {

      def fromByteString(bytes: ByteString): ParsingResult[DomainGovernanceTransaction] =
        for {
          converted <- TopologyTransaction.fromByteString(
            testedProtocolVersionValidation
          )(
            bytes
          )
          result <- converted match {
            case _: TopologyStateUpdate[_] =>
              Left(
                ProtoDeserializationError.TransactionDeserialization(
                  "Expecting DomainGovernanceTransaction, found TopologyStateUpdate"
                )
              )
            case domainGovernanceTransaction: DomainGovernanceTransaction =>
              Right(domainGovernanceTransaction)

          }
        } yield result

      val builder = (
          mapping: DomainGovernanceMapping,
          protocolVersion: ProtocolVersion,
      ) => DomainGovernanceTransaction(mapping, protocolVersion)

      testConversion(builder, fromByteString)(
        DomainParametersChange(DomainId(uid), defaultDynamicDomainParameters),
        Some(DomainParametersChange(DomainId(uid), defaultDynamicDomainParameters)),
      )
    }
  }

  "participant permission" should {
    "correctly determine lower of" in {
      ParticipantPermission.lowerOf(
        ParticipantPermission.Submission,
        ParticipantPermission.Observation,
      ) shouldBe ParticipantPermission.Observation
      ParticipantPermission.lowerOf(
        ParticipantPermission.Disabled,
        ParticipantPermission.Observation,
      ) shouldBe ParticipantPermission.Disabled
    }
  }

  "trust level" should {
    "correctly determine lower of" in {
      TrustLevel.lowerOf(TrustLevel.Vip, TrustLevel.Ordinary) shouldBe TrustLevel.Ordinary
    }
  }
}
