// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.transaction

import cats.syntax.apply.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{
  GeneratorsCrypto,
  Hash,
  PublicKey,
  Signature,
  SigningPublicKey,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{
  GeneratorsTopology,
  MediatorId,
  Namespace,
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{Generators, GeneratorsLf, LfPackageId}
import com.digitalasset.daml.lf.data.Ref.PackageId
import magnolify.scalacheck.auto.*
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.EitherValues.*

import scala.math.Ordering.Implicits.*

final class GeneratorsTransaction(
    protocolVersion: ProtocolVersion,
    generatorsProtocol: GeneratorsProtocol,
) {
  import GeneratorsCrypto.*
  import GeneratorsLf.*
  import generatorsProtocol.*
  import GeneratorsTopology.*
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val topologyChangeOpArb: Arbitrary[TopologyChangeOp] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[TopologyChangeOp.Replace],
      Arbitrary.arbitrary[TopologyChangeOp.Remove],
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
  implicit val topologyTransactionSigningPublicKeysArb: Arbitrary[NonEmpty[Seq[SigningPublicKey]]] =
    Arbitrary(Generators.nonEmptySetGen[SigningPublicKey].map(_.toSeq))
  implicit val topologyTransactionMappingsArb: Arbitrary[NonEmpty[Seq[TopologyMapping]]] =
    Arbitrary(Generators.nonEmptySetGen[TopologyMapping].map(_.toSeq))
  implicit val topologyTransactionPartyIdsArb: Arbitrary[NonEmpty[Seq[PartyId]]] =
    Arbitrary(Generators.nonEmptySetGen[PartyId].map(_.toSeq))
  implicit val topologyTransactionHostingParticipantsArb
      : Arbitrary[NonEmpty[Seq[HostingParticipant]]] =
    Arbitrary(Generators.nonEmptySetGen[HostingParticipant].map(_.toSeq))
  implicit val topologyTransactionVettedPackageArb: Arbitrary[VettedPackage] = Arbitrary(
    for {
      packageId <- Arbitrary.arbitrary[PackageId]
      validFrom <- Arbitrary.arbOption[CantonTimestamp].arbitrary
      validUntil <- Arbitrary
        .arbOption[CantonTimestamp]
        .arbitrary
        .suchThat(until => (validFrom, until).tupled.forall { case (from, until) => from < until })
    } yield VettedPackage(packageId, validFrom, validUntil)
  )

  implicit val hostingParticipantArb: Arbitrary[HostingParticipant] = Arbitrary(
    for {
      pid <- Arbitrary.arbitrary[ParticipantId]
      permission <- Arbitrary.arbitrary[ParticipantPermission]
    } yield HostingParticipant(pid, permission)
  )

  implicit val topologyMappingArb: Arbitrary[TopologyMapping] = genArbitrary

  implicit val decentralizedNamespaceDefinitionArb: Arbitrary[DecentralizedNamespaceDefinition] =
    Arbitrary(
      for {
        namespace <- Arbitrary.arbitrary[Namespace]
        owners <- Arbitrary.arbitrary[NonEmpty[Set[Namespace]]]
        // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
        threshold <- Gen.choose(1, owners.size).map(PositiveInt.tryCreate)
      } yield DecentralizedNamespaceDefinition.create(namespace, threshold, owners).value
    )

  implicit val mediatorSynchronizerStateArb: Arbitrary[MediatorSynchronizerState] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      group <- Arbitrary.arbitrary[NonNegativeInt]
      active <- Arbitrary.arbitrary[NonEmpty[Seq[MediatorId]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen.choose(1, active.size).map(PositiveInt.tryCreate)
      observers <- Arbitrary.arbitrary[NonEmpty[Seq[MediatorId]]]
    } yield MediatorSynchronizerState
      .create(synchronizerId, group, threshold, active, observers)
      .value
  )

  implicit val namespaceDelegationArb: Arbitrary[NamespaceDelegation] = Arbitrary(
    for {
      namespace <- Arbitrary.arbitrary[Namespace]
      target <- Arbitrary.arbitrary[SigningPublicKey]
      isRootDelegation <- // honor constraint that root delegation must be true if fingerprints match
        if (namespace.fingerprint == target.fingerprint) Gen.const(true) else Gen.oneOf(true, false)
    } yield NamespaceDelegation.create(namespace, target, isRootDelegation).value
  )

  implicit val purgeTopologyTransactionArb: Arbitrary[PurgeTopologyTransaction] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      mappings <- Arbitrary.arbitrary[NonEmpty[Seq[TopologyMapping]]]
    } yield PurgeTopologyTransaction.create(synchronizerId, mappings).value
  )

  implicit val partyToParticipantTopologyTransactionArb: Arbitrary[PartyToParticipant] = Arbitrary(
    for {
      partyId <- Arbitrary.arbitrary[PartyId]
      participants <- Arbitrary.arbitrary[NonEmpty[Seq[HostingParticipant]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen
        .choose(1, participants.count(_.permission >= ParticipantPermission.Confirmation).max(1))
        .map(PositiveInt.tryCreate)
    } yield PartyToParticipant
      .create(partyId, threshold, participants)
      .value
  )

  implicit val vettedPackagesTopologyTransactionArb: Arbitrary[VettedPackages] = Arbitrary(
    for {
      participantId <- Arbitrary.arbitrary[ParticipantId]
      vettedPackages <- Gen.listOf(Arbitrary.arbitrary[VettedPackage])
    } yield VettedPackages.create(participantId, vettedPackages).value
  )

  implicit val partyToKeyTopologyTransactionArb: Arbitrary[PartyToKeyMapping] = Arbitrary(
    for {
      partyId <- Arbitrary.arbitrary[PartyId]
      signingKeys <- Arbitrary.arbitrary[NonEmpty[Seq[SigningPublicKey]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen
        .choose(1, signingKeys.size)
        .map(PositiveInt.tryCreate)
    } yield PartyToKeyMapping
      .create(partyId, threshold, signingKeys)
      .value
  )

  implicit val sequencerSynchronizerStateArb: Arbitrary[SequencerSynchronizerState] = Arbitrary(
    for {
      synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
      active <- Arbitrary.arbitrary[NonEmpty[Seq[SequencerId]]]
      // Not using Arbitrary.arbitrary[PositiveInt] for threshold to honor constraint
      threshold <- Gen.choose(1, active.size).map(PositiveInt.tryCreate)
      observers <- Arbitrary.arbitrary[NonEmpty[Seq[SequencerId]]]
    } yield SequencerSynchronizerState.create(synchronizerId, threshold, active, observers).value
  )

  implicit val topologyTransactionArb
      : Arbitrary[TopologyTransaction[TopologyChangeOp, TopologyMapping]] = Arbitrary(
    for {
      op <- Arbitrary.arbitrary[TopologyChangeOp]
      serial <- Arbitrary.arbitrary[PositiveInt]
      mapping <- Arbitrary.arbitrary[TopologyMapping]
    } yield TopologyTransaction(op, serial, mapping, protocolVersion)
  )

  implicit val topologyTransactionSignaturesArb: Arbitrary[NonEmpty[Set[Signature]]] =
    Generators.nonEmptySet[Signature]

  implicit val txHashArb: Arbitrary[NonEmpty[Set[TxHash]]] = Arbitrary(
    Generators.nonEmptySet[Hash].arbitrary.map(_.map(TxHash(_)))
  )

  def multiTransactionSignaturesGen(transactionHash: TxHash) = for {
    hashes <- Arbitrary.arbitrary[NonEmpty[Set[TxHash]]]
    signatures <- Arbitrary.arbitrary[Signature]
  } yield MultiTransactionSignature(
    // Guarantees that the transaction hash is in the multi hash set
    NonEmpty.mk(Set, transactionHash, hashes.toSeq*),
    signatures,
  )

  def topologyTransactionSignatureArb(
      transactionHash: TxHash
  ): Arbitrary[TopologyTransactionSignature] = Arbitrary {
    Gen.frequency(
      (1, Arbitrary.arbitrary[Signature].map(SingleTransactionSignature(transactionHash, _))),
      (1, multiTransactionSignaturesGen(transactionHash)),
    )
  }

  implicit val signedTopologyTransactionArb
      : Arbitrary[SignedTopologyTransaction[TopologyChangeOp, TopologyMapping]] = Arbitrary(
    for {
      transaction <- Arbitrary.arbitrary[TopologyTransaction[TopologyChangeOp, TopologyMapping]]
      proposal <- Arbitrary.arbBool.arbitrary
      topologyTransactionSignatures <- {
        implicit val localSignatureArb = topologyTransactionSignatureArb(transaction.hash)
        Generators.nonEmptySetGen[TopologyTransactionSignature]
      }
    } yield SignedTopologyTransaction
      .create(transaction, topologyTransactionSignatures, proposal, protocolVersion)
  )

  implicit val signedTopologyTransactionsArb
      : Arbitrary[SignedTopologyTransactions[TopologyChangeOp, TopologyMapping]] = Arbitrary(
    for {
      transactions <- Gen.listOf(signedTopologyTransactionArb.arbitrary)
    } yield SignedTopologyTransactions(transactions, protocolVersion)
  )
}
