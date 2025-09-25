// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.interactive

import com.daml.ledger.api.v2.admin.party_management_service.AllocateExternalPartyRequest
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.BaseTest.{testedProtocolVersion, testedReleaseProtocolVersion}
import com.digitalasset.canton.FutureHelpers
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{CryptoConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.kms.CommunityKmsFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreFactory
import com.digitalasset.canton.interactive.ExternalPartyUtils.{
  ExternalParty,
  OnboardingTransactions,
}
import com.digitalasset.canton.logging.SuppressingLogger
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.{Namespace, ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.{NoReportingTracerProvider, TraceContext}
import com.google.protobuf.ByteString
import io.scalaland.chimney.dsl.*
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext

object ExternalPartyUtils {
  final case class ExternalParty(
      partyId: PartyId,
      signingFingerprints: NonEmpty[Seq[Fingerprint]],
  ) {
    def primitiveId: String = partyId.toProtoPrimitive
  }
  final case class OnboardingTransactions(
      partyToParticipant: TopologyTransaction[TopologyChangeOp.Replace, PartyToParticipant],
      multiHashSignatures: Seq[Signature],
      namespaceTransactions: Seq[(GenericTopologyTransaction, Seq[Signature])],
      partyToParticipantTransaction: (GenericTopologyTransaction, Seq[Signature]),
      partyToKeyMappingTransaction: (GenericTopologyTransaction, Seq[Signature]),
  ) {
    def toAllocateExternalPartyRequest(
        synchronizerId: SynchronizerId,
        identityProviderId: String = "",
    ): AllocateExternalPartyRequest =
      AllocateExternalPartyRequest(
        synchronizer = synchronizerId.toProtoPrimitive,
        onboardingTransactions =
          singleTransactionWithSignatures.map { case (transaction, signatures) =>
            AllocateExternalPartyRequest.SignedTransaction(
              transaction.getCryptographicEvidence,
              signatures.map(_.toProtoV30.transformInto[iss.Signature]),
            )
          },
        multiHashSignatures = multiHashSignatures.map(
          _.toProtoV30.transformInto[iss.Signature]
        ),
        identityProviderId = identityProviderId,
      )

    def singleTransactionWithSignatures: Seq[(GenericTopologyTransaction, Seq[Signature])] =
      namespaceTransactions ++
        Seq(partyToParticipantTransaction) ++
        Seq(partyToKeyMappingTransaction)
  }
}

trait ExternalPartyUtils extends FutureHelpers with EitherValues {

  def loggerFactory: SuppressingLogger
  def futureSupervisor: FutureSupervisor
  protected def timeouts: ProcessingTimeout
  def wallClock: WallClock

  implicit def externalPartyExecutionContext: ExecutionContext
  implicit protected def traceContext: TraceContext

  private val storage = new MemoryStorage(loggerFactory, timeouts)

  lazy val crypto: Crypto = Crypto
    .create(
      CryptoConfig(),
      storage,
      CryptoPrivateStoreFactory.withoutKms(wallClock, externalPartyExecutionContext),
      CommunityKmsFactory,
      testedReleaseProtocolVersion,
      nonStandardConfig = false,
      futureSupervisor,
      wallClock,
      externalPartyExecutionContext,
      timeouts,
      loggerFactory,
      NoReportingTracerProvider,
    )
    .valueOrFailShutdown("Failed to create crypto object")
    .futureValue

  def generateProtocolSigningKeys(numberOfKeys: PositiveInt) =
    NonEmpty
      .from(
        Seq.fill(numberOfKeys.value)(
          crypto.generateSigningKey(usage = SigningKeyUsage.ProtocolOnly).futureValueUS.value
        )
      )
      .getOrElse(
        fail("Expected at least one protocol signing key")
      )

  def generateNamespaceSigningKey: SigningPublicKey =
    crypto.generateSigningKey(usage = SigningKeyUsage.NamespaceOnly).futureValueUS.value

  def generateExternalPartyOnboardingTransactions(
      name: String,
      confirming: ParticipantId,
      extraConfirming: Seq[ParticipantId] = Seq.empty,
      observing: Seq[ParticipantId] = Seq.empty,
      confirmationThreshold: PositiveInt = PositiveInt.one,
      numberOfKeys: PositiveInt = PositiveInt.one,
      keyThreshold: PositiveInt = PositiveInt.one,
      decentralizedOwners: Option[PositiveInt] = None,
  ): (OnboardingTransactions, ExternalParty) = {

    val protocolSigningKeys: NonEmpty[Seq[SigningPublicKey]] = generateProtocolSigningKeys(
      numberOfKeys
    )

    val (namespaceOwners, namespaceTxsWithSignatures, partyId) =
      decentralizedOwners match {
        case Some(nbNamespaces) =>
          // Decentralized namespace owners root namespaces
          val namespaceOwnerTxsAndSignatures = (1 to nbNamespaces.value).map { _ =>
            val key = generateNamespaceSigningKey
            val namespace = Namespace(key.fingerprint)
            val transaction = TopologyTransaction(
              TopologyChangeOp.Replace,
              serial = PositiveInt.one,
              NamespaceDelegation.tryCreate(
                namespace = namespace,
                target = key,
                restriction = DelegationRestriction.CanSignAllMappings,
              ),
              testedProtocolVersion,
            )
            // Sign them individually, as we expect each namespace owner would
            val signatures = signTxAs(
              transaction.hash.hash.getCryptographicEvidence,
              Seq(key.fingerprint),
              keyUsage = SigningKeyUsage.NamespaceOnly,
            )
            transaction -> signatures
          }

          val namespaceOwners = namespaceOwnerTxsAndSignatures.map(_._1.mapping.namespace).toSet
          val decentralizedNamespace =
            DecentralizedNamespaceDefinition.computeNamespace(namespaceOwners)

          // Create the decentralized namespace transaction from the namespace owners
          val decentralizedNamespaceTransaction = TopologyTransaction(
            TopologyChangeOp.Replace,
            serial = PositiveInt.one,
            DecentralizedNamespaceDefinition.tryCreate(
              decentralizedNamespace = decentralizedNamespace,
              threshold = nbNamespaces,
              owners = NonEmpty
                .from(namespaceOwners)
                .getOrElse(fail("Expected non empty decentralized namespace")),
            ),
            testedProtocolVersion,
          )
          val partyId = PartyId.tryCreate(name, decentralizedNamespace.fingerprint)

          // Also have each namespace owner sign the decentralized namespace transaction
          val decentralizedNamespaceOwnerSignatures = namespaceOwners.flatMap { namespaceOwner =>
            signTxAs(
              decentralizedNamespaceTransaction.hash.hash.getCryptographicEvidence,
              Seq(namespaceOwner.fingerprint),
              keyUsage = SigningKeyUsage.NamespaceOnly,
            )
          }.toSeq
          (
            namespaceOwners,
            namespaceOwnerTxsAndSignatures :+ (decentralizedNamespaceTransaction, decentralizedNamespaceOwnerSignatures),
            partyId,
          )
        case None =>
          // For a non-decentralized namespace, create a simple root NamespaceDelegation
          val namespaceKey: SigningPublicKey =
            crypto.generateSigningKey(usage = SigningKeyUsage.NamespaceOnly).futureValueUS.value
          val partyId: PartyId = PartyId.tryCreate(name, namespaceKey.fingerprint)
          val namespaceTransaction = TopologyTransaction(
            TopologyChangeOp.Replace,
            serial = PositiveInt.one,
            NamespaceDelegation.tryCreate(
              namespace = partyId.namespace,
              target = namespaceKey,
              CanSignAllMappings,
            ),
            testedProtocolVersion,
          )
          (
            Seq(namespaceTransaction.mapping.namespace),
            Seq(namespaceTransaction -> Seq.empty),
            partyId,
          )
      }

    val allConfirming = NonEmpty.mk(Seq, confirming, extraConfirming*)
    val confirmingHostingParticipants = allConfirming.forgetNE.map { cp =>
      HostingParticipant(
        cp,
        ParticipantPermission.Confirmation,
      )
    }
    val observingHostingParticipants = observing.map { op =>
      HostingParticipant(
        op,
        ParticipantPermission.Observation,
      )
    }
    val partyToParticipantTx =
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToParticipant.tryCreate(
          partyId = partyId,
          threshold = confirmationThreshold,
          participants = confirmingHostingParticipants ++ observingHostingParticipants,
        ),
        testedProtocolVersion,
      )

    val partyToKeyTx =
      TopologyTransaction(
        TopologyChangeOp.Replace,
        serial = PositiveInt.one,
        PartyToKeyMapping.tryCreate(
          partyId = partyId,
          threshold = keyThreshold,
          signingKeys = protocolSigningKeys,
        ),
        testedProtocolVersion,
      )

    // The combined hash covers all unsigned transactions
    val transactionHashes =
      NonEmpty.mk(Set, partyToParticipantTx.hash, partyToKeyTx.hash) ++ namespaceTxsWithSignatures
        .map(
          _._1.hash
        )
    val combinedMultiTxHash =
      MultiTransactionSignature.computeCombinedHash(transactionHashes, crypto.pureCrypto)

    // Sign the multi hash with the namespace keys, as it is needed to authorize all transactions
    val namespaceOwnerSignatures = namespaceOwners.map { namespace =>
      crypto.privateCrypto
        .sign(
          combinedMultiTxHash,
          namespace.fingerprint,
          NonEmpty.mk(Set, SigningKeyUsage.Namespace),
        )
        .futureValueUS
        .value
    }

    // The protocol key signature is only needed on the party to key mapping, so we can sign only that, and combine it with the
    // namespace signatures
    val protocolSignatures = protocolSigningKeys.map { key =>
      crypto.privateCrypto
        .sign(
          partyToKeyTx.hash.hash,
          key.fingerprint,
          NonEmpty.mk(Set, SigningKeyUsage.Protocol),
        )
        .futureValueUS
        .value
    }

    (
      OnboardingTransactions(
        partyToParticipantTx,
        namespaceOwnerSignatures.toSeq,
        namespaceTxsWithSignatures,
        partyToParticipantTx -> Seq.empty,
        partyToKeyTx -> protocolSignatures.forgetNE,
      ),
      ExternalParty(partyId, protocolSigningKeys.map(_.fingerprint)),
    )
  }

  def signTopologyTransaction[Op <: TopologyChangeOp, M <: TopologyMapping](
      fingerprint: Fingerprint,
      topologyTransaction: TopologyTransaction[Op, M],
  ): SignedTopologyTransaction[Op, M] =
    SignedTopologyTransaction
      .signAndCreate(
        topologyTransaction,
        NonEmpty.mk(Set, fingerprint),
        isProposal = false,
        crypto.privateCrypto,
        testedProtocolVersion,
      )
      .futureValueUS
      .value

  def signTxAs(
      hash: ByteString,
      signingFingerprints: Seq[Fingerprint],
      keyUsage: NonEmpty[Set[SigningKeyUsage]] = SigningKeyUsage.ProtocolOnly,
  ): Seq[Signature] = {
    val signatures =
      signingFingerprints.map { fingerprint =>
        crypto.privateCrypto
          .signBytes(
            hash,
            fingerprint,
            keyUsage,
          )
          .valueOrFailShutdown("Failed to sign transaction hash")
          .futureValue
      }

    signatures
  }

}
