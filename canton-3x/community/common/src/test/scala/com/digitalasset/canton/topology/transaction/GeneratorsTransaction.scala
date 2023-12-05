// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import better.files.File
import com.digitalasset.canton.crypto.{Signature, SigningPublicKey, X509CertificatePem}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.testing.utils.TestResourceUtils
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

object GeneratorsTransaction {
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.protocol.GeneratorsProtocol.*
  import com.digitalasset.canton.version.GeneratorsVersion.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*

  implicit val addRemoveChangeOp: Arbitrary[AddRemoveChangeOp] = genArbitrary
  implicit val topologyChangeOpArb: Arbitrary[TopologyChangeOp] = genArbitrary

  implicit val ownerToKeyMappingArb: Arbitrary[OwnerToKeyMapping] = genArbitrary
  private lazy val legalIdentityClaimEvidence: LegalIdentityClaimEvidence = {
    val pemPath = TestResourceUtils.resourceFile("tls/participant.pem").toPath
    LegalIdentityClaimEvidence.X509Cert(X509CertificatePem.tryFromFile(File(pemPath)))
  }
  implicit val legalIdentityClaimArb: Arbitrary[LegalIdentityClaim] = Arbitrary(
    for {
      pv <- Arbitrary.arbitrary[ProtocolVersion]
      uid <- Arbitrary.arbitrary[UniqueIdentifier]
      evidence = legalIdentityClaimEvidence
    } yield LegalIdentityClaim.create(uid, evidence, pv)
  )
  implicit val signedLegalIdentityClaimArb: Arbitrary[SignedLegalIdentityClaim] = Arbitrary(
    for {
      legalIdentityClaim <- Arbitrary.arbitrary[LegalIdentityClaim]
      signature <- Arbitrary.arbitrary[Signature]
    } yield SignedLegalIdentityClaim.create(legalIdentityClaim, signature)
  )
  implicit val vettedPackagesArb: Arbitrary[VettedPackages] = genArbitrary

  // If the pattern match is not exhaustive, update the list below and the generator of ParticipantState
  {
    ((_: TrustLevel) match {
      case TrustLevel.Ordinary => ()
      case TrustLevel.Vip => ()
    }).discard
  }
  private val trustLevels: Seq[TrustLevel] = Seq(TrustLevel.Vip, TrustLevel.Ordinary)
  implicit val participantStateArb: Arbitrary[ParticipantState] = Arbitrary(for {
    side <- Arbitrary.arbitrary[RequestSide]
    domain <- Arbitrary.arbitrary[DomainId]
    participant <- Arbitrary.arbitrary[ParticipantId]
    permission <- Arbitrary.arbitrary[ParticipantPermission]
    trustLevel <-
      if (permission.canConfirm) Gen.oneOf(trustLevels) else Gen.const(TrustLevel.Ordinary)
  } yield ParticipantState(side, domain, participant, permission, trustLevel))

  implicit val topologyStateUpdateMappingArb: Arbitrary[TopologyStateUpdateMapping] = genArbitrary
  implicit val topologyStateUpdateElementArb: Arbitrary[TopologyStateUpdateElement] = genArbitrary

  def domainParametersChangeGenFor(pv: ProtocolVersion): Gen[DomainParametersChange] = for {
    domainId <- Arbitrary.arbitrary[DomainId]
    parameters <- GeneratorsProtocol.dynamicDomainParametersGenFor(pv)
  } yield DomainParametersChange(domainId, parameters)

  // If the pattern match is not exhaustive, update generator below
  {
    ((_: DomainGovernanceMapping) match {
      case _: DomainParametersChange => ()
    }).discard
  }
  def domainGovernanceMappingGenFor(pv: ProtocolVersion): Gen[DomainGovernanceMapping] =
    domainParametersChangeGenFor(pv)

  def domainGovernanceElementGenFor(pv: ProtocolVersion): Gen[DomainGovernanceElement] =
    domainGovernanceMappingGenFor(pv).map(DomainGovernanceElement)
  implicit val domainGovernanceElementArb: Arbitrary[DomainGovernanceElement] = genArbitrary

  implicit val topologyStateUpdateArb: Arbitrary[TopologyStateUpdate[AddRemoveChangeOp]] =
    Arbitrary(for {
      op <- Arbitrary.arbitrary[AddRemoveChangeOp]
      element <- Arbitrary.arbitrary[TopologyStateUpdateElement]
      rpv <- representativeProtocolVersionGen(TopologyTransaction)
    } yield TopologyStateUpdate(op, element, rpv.representative))

  implicit val domainGovernanceTransactionArb: Arbitrary[DomainGovernanceTransaction] =
    Arbitrary(for {
      rpv <- representativeProtocolVersionGen(TopologyTransaction)
      element <- domainGovernanceElementGenFor(rpv.representative)
    } yield DomainGovernanceTransaction(element, rpv.representative))
  def domainGovernanceTransactionGenFor(pv: ProtocolVersion): Gen[DomainGovernanceTransaction] =
    for {
      element <- domainGovernanceElementGenFor(pv)
    } yield DomainGovernanceTransaction(element, pv)

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: TopologyTransaction[TopologyChangeOp]) match {
      case _: TopologyStateUpdate[_] => ()
      case _: DomainGovernanceTransaction => ()
    }).discard
  }
  implicit val topologyTransactionArb: Arbitrary[TopologyTransaction[TopologyChangeOp]] = Arbitrary(
    Gen.oneOf[TopologyTransaction[TopologyChangeOp]](
      topologyStateUpdateArb.arbitrary,
      domainGovernanceTransactionArb.arbitrary,
    )
  )

  def signedTopologyTransactionGenFor(
      pv: ProtocolVersion
  ): Gen[SignedTopologyTransaction[TopologyChangeOp]] = for {
    transaction <- Arbitrary.arbitrary[TopologyTransaction[TopologyChangeOp]]
    key <- Arbitrary.arbitrary[SigningPublicKey]
    signature <- Arbitrary.arbitrary[Signature]
  } yield SignedTopologyTransaction(
    transaction,
    key,
    signature,
    SignedTopologyTransaction.protocolVersionRepresentativeFor(pv),
  )

  implicit val signedTopologyTransactionArb
      : Arbitrary[SignedTopologyTransaction[TopologyChangeOp]] = Arbitrary(for {
    rpv <- representativeProtocolVersionGen(SignedTopologyTransaction)
    tx <- signedTopologyTransactionGenFor(rpv.representative)
  } yield tx)
}
