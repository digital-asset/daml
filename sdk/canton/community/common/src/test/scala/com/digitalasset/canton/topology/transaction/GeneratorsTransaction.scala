// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  SigningKeyUsage,
  SigningKeysWithThreshold,
  SigningPublicKey,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.sequencing.GeneratorsSequencing
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
}
import com.digitalasset.canton.topology.transaction.DelegationRestriction.{
  CanSignAllButNamespaceDelegations,
  CanSignAllMappings,
  CanSignSpecificMappings,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyTransaction.TxHash
import com.digitalasset.canton.topology.{
  GeneratorsTopology,
  MediatorId,
  Member,
  Namespace,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
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
    generatorsLf: GeneratorsLf,
    generatorsProtocol: GeneratorsProtocol,
    generatorsTopology: GeneratorsTopology,
    generatorsSequencing: GeneratorsSequencing,
) {
  import GeneratorsCrypto.*
  import generatorsLf.*
  import generatorsProtocol.*
  import generatorsSequencing.*
  import generatorsTopology.*
  import Generators.*
  import com.digitalasset.canton.config.GeneratorsConfig.*

  implicit val topologyChangeOpArb: Arbitrary[TopologyChangeOp] = Arbitrary(
    Gen.oneOf(TopologyChangeOp.Replace, TopologyChangeOp.Remove)
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
      onboarding <- Arbitrary.arbBool.arbitrary
    } yield HostingParticipant(pid, permission, onboarding)
  )

  implicit val synchronizerUpgradeAnnouncementArb: Arbitrary[SynchronizerUpgradeAnnouncement] =
    Arbitrary(for {
      psid <- Arbitrary.arbitrary[PhysicalSynchronizerId]
      upgradeTime <- Arbitrary.arbitrary[CantonTimestamp]
    } yield SynchronizerUpgradeAnnouncement(psid, upgradeTime))

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

  val restrictionWithNamespaceDelegation: Gen[DelegationRestriction] = Gen.oneOf(
    Gen.const(CanSignAllMappings),
    Generators
      // generate a random sequence of codes,
      .nonEmptySetGen[TopologyMapping.Code]
      // but always add the namespace delegation code, so that the result can always sign namespace delegations
      .map(_ incl TopologyMapping.Code.NamespaceDelegation)
      .map(CanSignSpecificMappings(_)),
  )

  val restrictionWithoutNamespaceDelegation: Gen[DelegationRestriction] = Gen.oneOf(
    Gen.const(CanSignAllButNamespaceDelegations),
    Generators
      .nonEmptySetGen(
        Arbitrary(
          // generate a random set of codes but don't allow for NamespaceDelegation to be included
          Gen.oneOf(TopologyMapping.Code.all.toSet - TopologyMapping.Code.NamespaceDelegation)
        )
      )
      .map(CanSignSpecificMappings(_)),
  )

  implicit val namespaceDelegationArb: Arbitrary[NamespaceDelegation] = Arbitrary(
    for {
      namespace <- Arbitrary.arbitrary[Namespace]
      // target key must include the `Namespace` usage
      target <- Arbitrary
        .arbitrary[SigningPublicKey]
        .retryUntil(key =>
          SigningKeyUsage.matchesRelevantUsages(key.usage, SigningKeyUsage.NamespaceOnly)
        )
      delegationRestriction <- // honor constraint that the delegation must be able to sign namespace delegations if fingerprints match
        if (namespace.fingerprint == target.fingerprint)
          restrictionWithNamespaceDelegation
        else restrictionWithoutNamespaceDelegation
    } yield NamespaceDelegation.tryCreate(namespace, target, delegationRestriction)
  )

  implicit val signingKeysWithThresholdArb: Arbitrary[SigningKeysWithThreshold] = Arbitrary(
    for {
      signingKeys <- nonEmptyListGen[SigningPublicKey]
      threshold <- Gen
        .choose(1, signingKeys.size)
        .map(PositiveInt.tryCreate)
    } yield SigningKeysWithThreshold.tryCreate(
      signingKeys,
      threshold,
    )
  )

  implicit val partyToParticipantTopologyTransactionArb: Arbitrary[PartyToParticipant] = Arbitrary(
    for {
      partyId <- Arbitrary.arbitrary[PartyId]
      participants <- nonEmptyListGen[HostingParticipant]
      threshold <- Gen
        .choose(1, participants.count(_.permission >= ParticipantPermission.Confirmation).max(1))
        .map(PositiveInt.tryCreate)
      signingKeysWithThreshold <- Gen.option(Arbitrary.arbitrary[SigningKeysWithThreshold])
    } yield PartyToParticipant
      .create(partyId, threshold, participants, signingKeysWithThreshold)
      .value
  )

  implicit val vettedPackagesTopologyTransactionArb: Arbitrary[VettedPackages] = Arbitrary(
    for {
      participantId <- Arbitrary.arbitrary[ParticipantId]
      vettedPackages <- boundedListGen[VettedPackage]
    } yield VettedPackages.create(participantId, vettedPackages).value
  )

  implicit val ownerToKeyTopologyTransactionArb: Arbitrary[OwnerToKeyMapping] = Arbitrary(
    for {
      member <- Arbitrary.arbitrary[Member]
      keys <- Arbitrary.arbitrary[NonEmpty[Seq[PublicKey]]]
    } yield OwnerToKeyMapping.create(member, keys).value
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

  def multiTransactionSignaturesGen(transactionHash: TxHash): Gen[MultiTransactionSignature] = for {
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

      signatures <- {
        implicit val localSignatureArb: Arbitrary[TopologyTransactionSignature] =
          topologyTransactionSignatureArb(transaction.hash)
        Generators.nonEmptyListGen[TopologyTransactionSignature]
      }

    } yield SignedTopologyTransaction
      .withTopologySignatures(transaction, signatures, proposal, protocolVersion)
  )

  implicit val signedTopologyTransactionsArb
      : Arbitrary[SignedTopologyTransactions[TopologyChangeOp, TopologyMapping]] = Arbitrary(
    for {
      transactions <- boundedListGen[GenericSignedTopologyTransaction]
    } yield SignedTopologyTransactions(transactions, protocolVersion)
  )

  implicit val storedTopologyTransactionsArb
      : Arbitrary[StoredTopologyTransactions[TopologyChangeOp, TopologyMapping]] = Arbitrary(
    for {
      transactions <- boundedListGen[StoredTopologyTransaction[TopologyChangeOp, TopologyMapping]]
    } yield StoredTopologyTransactions(transactions)
  )
}
