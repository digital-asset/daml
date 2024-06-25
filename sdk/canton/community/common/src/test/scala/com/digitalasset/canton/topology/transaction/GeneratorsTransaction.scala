// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import better.files.File
import com.digitalasset.canton.crypto.{Signature, SigningPublicKey, X509CertificatePem}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.testing.utils.TestResourceUtils
import com.digitalasset.canton.topology.{DomainId, Namespace, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsTransaction(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import com.digitalasset.canton.GeneratorsLf.*
  import com.digitalasset.canton.config.GeneratorsConfig.*
  import com.digitalasset.canton.crypto.GeneratorsCrypto.*
  import com.digitalasset.canton.topology.GeneratorsTopology.*
  import generatorsProtocol.*

  implicit val addRemoveChangeOp: Arbitrary[AddRemoveChangeOp] = genArbitrary
  implicit val topologyChangeOpArb: Arbitrary[TopologyChangeOp] = genArbitrary

  implicit val ownerToKeyMappingArb: Arbitrary[OwnerToKeyMapping] = genArbitrary
  private lazy val legalIdentityClaimEvidence: LegalIdentityClaimEvidence = {
    val pemPath = TestResourceUtils.resourceFile("tls/participant.pem").toPath
    LegalIdentityClaimEvidence.X509Cert(X509CertificatePem.tryFromFile(File(pemPath)))
  }
  implicit val legalIdentityClaimArb: Arbitrary[LegalIdentityClaim] = Arbitrary(
    for {
      uid <- Arbitrary.arbitrary[UniqueIdentifier]
      evidence = legalIdentityClaimEvidence
    } yield LegalIdentityClaim.create(uid, evidence, protocolVersion)
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

  implicit val namespaceDelegationArb: Arbitrary[NamespaceDelegation] = Arbitrary(
    for {
      isRootDelegation <- Arbitrary.arbitrary[Boolean]
      target <- signingPublicKeyArb.arbitrary
      namespace <-
        if (!isRootDelegation) {
          Arbitrary.arbitrary[Namespace] suchThat (_.fingerprint != target.fingerprint)
        } else Arbitrary.arbitrary[Namespace]
    } yield NamespaceDelegation(
      isRootDelegation = isRootDelegation,
      namespace = namespace,
      target = target,
    )
  )

  implicit val topologyStateUpdateMappingArb: Arbitrary[TopologyStateUpdateMapping] = genArbitrary
  implicit val topologyStateUpdateElementArb: Arbitrary[TopologyStateUpdateElement] = genArbitrary

  implicit val domainParametersChangeArb: Arbitrary[DomainParametersChange] = genArbitrary

  // If the pattern match is not exhaustive, update generator below
  {
    ((_: DomainGovernanceMapping) match {
      case _: DomainParametersChange => ()
    }).discard
  }
  implicit val domainGovernanceMappingArb: Arbitrary[DomainGovernanceMapping] = genArbitrary

  implicit val domainGovernanceElementArb: Arbitrary[DomainGovernanceElement] = genArbitrary

  implicit val topologyStateUpdateArb: Arbitrary[TopologyStateUpdate[AddRemoveChangeOp]] =
    Arbitrary(for {
      op <- Arbitrary.arbitrary[AddRemoveChangeOp]
      element <- Arbitrary.arbitrary[TopologyStateUpdateElement]
    } yield TopologyStateUpdate(op, element, protocolVersion))

  implicit val domainGovernanceTransactionArb: Arbitrary[DomainGovernanceTransaction] = {
    val rpv = TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
    Arbitrary(for {
      element <- domainGovernanceElementArb.arbitrary
    } yield DomainGovernanceTransaction(element, rpv.representative))
  }

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

  implicit val signedTopologyTransactionArb
      : Arbitrary[SignedTopologyTransaction[TopologyChangeOp]] = Arbitrary(
    for {
      transaction <- Arbitrary.arbitrary[TopologyTransaction[TopologyChangeOp]]
      key <- Arbitrary.arbitrary[SigningPublicKey]
      signature <- Arbitrary.arbitrary[Signature]
    } yield SignedTopologyTransaction(
      transaction,
      key,
      signature,
      SignedTopologyTransaction.protocolVersionRepresentativeFor(protocolVersion),
    )
  )

}
