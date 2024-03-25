// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveLong}
import com.digitalasset.canton.crypto.{GeneratorsCrypto, PublicKey, Signature, SigningPublicKey}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.{
  DomainId,
  GeneratorsTopology,
  MediatorId,
  Member,
  Namespace,
  SequencerId,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{Generators, GeneratorsLf, LfPackageId}
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues.*

final class GeneratorsTransaction(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import GeneratorsCrypto.*
  import GeneratorsLf.*
  import generatorsProtocol.*
  import GeneratorsTopology.*
  import com.digitalasset.canton.config.GeneratorsConfig.{nonNegativeFiniteDurationArb as _, *}

  implicit val topologyChangeOpArb: Arbitrary[TopologyChangeOpX] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[TopologyChangeOpX.Replace],
      Arbitrary.arbitrary[TopologyChangeOpX.Remove],
    )
  )

  implicit val topologyTransactionNamespacesArb: Arbitrary[NonEmpty[Set[Namespace]]] =
    Generators.nonEmptySet[Namespace]
  implicit val topologyTransactionMediatorIdsArb: Arbitrary[NonEmpty[Seq[MediatorId]]] =
    Arbitrary(Generators.nonEmptySetGen[MediatorId].map(_.toSeq))
  implicit val topologyTransactionSequencerIdsArb: Arbitrary[NonEmpty[Seq[SequencerId]]] =
    Arbitrary(Generators.nonEmptySetGen[SequencerId].map(_.toSeq))
  implicit val topologyTransactionLfPackageIdsArb: Arbitrary[NonEmpty[Seq[LfPackageId]]] =
    Arbitrary(Generators.nonEmptySetGen[LfPackageId].map(_.toSeq))
  implicit val topologyTransactionPublicKeysArb: Arbitrary[NonEmpty[Seq[PublicKey]]] =
    Arbitrary(Generators.nonEmptySetGen[PublicKey].map(_.toSeq))
  implicit val topologyTransactionMappingsArb: Arbitrary[NonEmpty[Seq[TopologyMappingX]]] =
    Arbitrary(Generators.nonEmptySetGen[TopologyMappingX].map(_.toSeq))

  implicit val topologyMappingArb: Arbitrary[TopologyMappingX] = genArbitrary

  implicit val decentralizedNamespaceDefinitionArb: Arbitrary[DecentralizedNamespaceDefinitionX] =
    Arbitrary(
      for {
        namespace <- Arbitrary.arbitrary[Namespace]
        owners <- Arbitrary.arbitrary[NonEmpty[Set[Namespace]]]
        // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
        threshold <- Gen.choose(1, owners.size).map(PositiveInt.tryCreate)
      } yield DecentralizedNamespaceDefinitionX.create(namespace, threshold, owners).value
    )

  implicit val mediatorDomainStateXArb: Arbitrary[MediatorDomainStateX] = Arbitrary(
    for {
      domainId <- Arbitrary.arbitrary[DomainId]
      group <- Arbitrary.arbitrary[NonNegativeInt]
      active <- Arbitrary.arbitrary[NonEmpty[Seq[MediatorId]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen.choose(1, active.size).map(PositiveInt.tryCreate)
      observers <- Arbitrary.arbitrary[NonEmpty[Seq[MediatorId]]]
    } yield MediatorDomainStateX.create(domainId, group, threshold, active, observers).value
  )

  implicit val namespaceDelegationXArb: Arbitrary[NamespaceDelegationX] = Arbitrary(
    for {
      namespace <- Arbitrary.arbitrary[Namespace]
      target <- Arbitrary.arbitrary[SigningPublicKey]
      isRootDelegation <- // honor constraint that root delegation must be true if fingerprints match
        if (namespace.fingerprint == target.fingerprint) Gen.const(true) else Gen.oneOf(true, false)
    } yield NamespaceDelegationX.create(namespace, target, isRootDelegation).value
  )

  implicit val purgeTopologyTransactionXArb: Arbitrary[PurgeTopologyTransactionX] = Arbitrary(
    for {
      domain <- Arbitrary.arbitrary[DomainId]
      mappings <- Arbitrary.arbitrary[NonEmpty[Seq[TopologyMappingX]]]
    } yield PurgeTopologyTransactionX.create(domain, mappings).value
  )

  implicit val sequencerDomainStateXArb: Arbitrary[SequencerDomainStateX] = Arbitrary(
    for {
      domain <- Arbitrary.arbitrary[DomainId]
      active <- Arbitrary.arbitrary[NonEmpty[Seq[SequencerId]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen.choose(1, active.size).map(PositiveInt.tryCreate)
      observers <- Arbitrary.arbitrary[NonEmpty[Seq[SequencerId]]]
    } yield SequencerDomainStateX.create(domain, threshold, active, observers).value
  )

  implicit val trafficControlStateXArb: Arbitrary[TrafficControlStateX] = Arbitrary(
    for {
      domain <- Arbitrary.arbitrary[DomainId]
      member <- Arbitrary.arbitrary[Member]
      totalExtraTrafficLimit <- Arbitrary.arbitrary[PositiveLong]
    } yield TrafficControlStateX.create(domain, member, totalExtraTrafficLimit).value
  )

  implicit val topologyTransactionArb
      : Arbitrary[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]] = Arbitrary(
    for {
      op <- Arbitrary.arbitrary[TopologyChangeOpX]
      serial <- Arbitrary.arbitrary[PositiveInt]
      mapping <- Arbitrary.arbitrary[TopologyMappingX]
    } yield TopologyTransactionX(op, serial, mapping, protocolVersion)
  )

  implicit val topologyTransactionSignaturesArb: Arbitrary[NonEmpty[Set[Signature]]] =
    Generators.nonEmptySet[Signature]

  implicit val signedTopologyTransactionArb
      : Arbitrary[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]] = Arbitrary(
    for {
      transaction <- Arbitrary.arbitrary[TopologyTransactionX[TopologyChangeOpX, TopologyMappingX]]
      signatures <- Arbitrary.arbitrary[NonEmpty[Set[Signature]]]
      proposal <- Arbitrary.arbBool.arbitrary
    } yield SignedTopologyTransactionX(transaction, signatures, proposal)(
      SignedTopologyTransactionX.protocolVersionRepresentativeFor(protocolVersion)
    )
  )

}
