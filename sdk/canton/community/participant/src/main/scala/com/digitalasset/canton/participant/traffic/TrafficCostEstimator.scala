// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.traffic

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.KeyPurpose.Signing
import com.digitalasset.canton.crypto.SigningError.InvariantViolation
import com.digitalasset.canton.crypto.signer.{SyncCryptoSigner, SyncCryptoSignerWithSessionKeys}
import com.digitalasset.canton.crypto.store.CryptoPrivateStore
import com.digitalasset.canton.crypto.{
  CryptoKeyFormat,
  Fingerprint,
  Hash,
  HashPurpose,
  Signature,
  SignatureDelegation,
  SignatureDelegationValidityPeriod,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SigningKeyUsage,
  SigningKeysWithThreshold,
  SigningPublicKey,
  SyncCryptoError,
  SynchronizerCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.ledger.participant.state.SyncService.SubmissionCostEstimation
import com.digitalasset.canton.ledger.participant.state.{SubmitterInfo, TransactionMeta}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.TransactionProcessingSteps
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.ContractInstanceOfId
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.participant.traffic.TrafficCostEstimator.{
  MockCryptoSigner,
  mediatorGroupIndex,
  mediatorGroupRecipient,
  mockSignature,
  rootHash,
}
import com.digitalasset.canton.platform.apiserver.services.command.interactive.CostEstimationHints
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponse,
  ConfirmationResponses,
  LocalApprove,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{
  ContractInstance,
  LfContractId,
  LfFatContractInst,
  LfVersionedTransaction,
  RequestId,
  RootHash,
  TransactionMetadata,
  WellFormedTransaction,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, MediatorGroupRecipient, Recipients}
import com.digitalasset.canton.sequencing.traffic.TrafficStateController
import com.digitalasset.canton.store.SessionKeyStore
import com.digitalasset.canton.topology.MediatorGroup.MediatorGroupIndex
import com.digitalasset.canton.topology.client.{
  SynchronizerTopologyClientWithInit,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HashingSchemeVersion.V2
import com.digitalasset.canton.{LedgerSubmissionId, LfKeyResolver, WorkflowId}
import com.google.crypto.tink.subtle.Ed25519Sign
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.util.Random

/** Provides traffic cost estimation of a transaction submission (confirmation request + response)
  * Current assumptions and known limitations: Exact addressing of confirmation request and response
  * depends on whether the executing participant hosts the relevant parties. To simplify, the logic
  * assumes that this node will be the executing participant. This also has an impact on the number
  * of session encryption keys. External signatures number and type impact the final payload size
  * and therefore cost. Hints can be provided to improve accuracy. In the absence of hints, the
  * estimation uses the threshold-first keys registered in the external party's PartyToKeyMapping
  */
class TrafficCostEstimator(
    confirmationRequestFactory: TransactionConfirmationRequestFactory,
    topologyClient: SynchronizerTopologyClientWithInit,
    synchronizerCrypto: SynchronizerCryptoClient,
    contractStore: ContractStore,
    sessionKeyStore: SessionKeyStore,
    psid: PhysicalSynchronizerId,
    participantId: ParticipantId,
    trafficStateController: TrafficStateController,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  def estimateTrafficCost(
      transaction: LfVersionedTransaction,
      transactionMeta: TransactionMeta,
      submitterInfo: SubmitterInfo,
      keyResolver: LfKeyResolver,
      disclosedContracts: Map[LfContractId, LfFatContractInst],
      costHints: CostEstimationHints,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SubmissionCostEstimation] = {
    val snapshot: TopologySnapshot = topologyClient.headSnapshot

    // Create a crypto synchronizer object which overrides the crypto signer with a mock one to avoid
    // potentially expensive KMS calls.
    val client: SynchronizerSnapshotSyncCryptoApi = synchronizerCrypto.createWithCustomCryptoSigner(
      snapshot,
      { currentSigner =>
        val hasSessionKeys = currentSigner match {
          case _: SyncCryptoSignerWithSessionKeys => true
          case _ => false
        }
        new MockCryptoSigner(
          participantId,
          synchronizerCrypto.pureCrypto.signingAlgorithmSpecs.allowed,
          hasSessionKeys,
          loggerFactory,
        )
      },
    )

    def estimateCost = for {
      transactionMetadata <- EitherT.fromEither[FutureUnlessShutdown](
        TransactionMetadata.fromTransactionMeta(
          transactionMeta.ledgerEffectiveTime,
          transactionMeta.preparationTime,
          transactionMeta.optNodeSeeds,
        )
      )
      wfTransaction <- EitherT.fromEither[FutureUnlessShutdown](
        WellFormedTransaction.check(transaction, transactionMetadata, WithoutSuffixes)
      )
      disclosedContractInstances <- EitherT.fromEither[FutureUnlessShutdown](
        disclosedContracts.toList
          .parTraverse { case (cid, fci) =>
            ContractInstance.create(fci).map(cid -> _)
          }
          .map(_.toMap)
      )
      confirmationRequestEstimatedCost <- estimateConfirmationRequestCost(
        trafficStateController,
        submitterInfo,
        client,
        snapshot,
        wfTransaction,
        keyResolver,
        costHints,
        disclosedContractInstances,
      )
      confirmationResponseEstimatedCost <- estimateConfirmationResponseCost(
        trafficStateController,
        submitterInfo,
        client,
        snapshot,
      )
    } yield SubmissionCostEstimation(
      snapshot.timestamp,
      confirmationRequestEstimatedCost,
      confirmationResponseEstimatedCost,
    )

    EitherT
      .liftF[FutureUnlessShutdown, String, Boolean](
        snapshot.trafficControlParameters(psid.protocolVersion).map(_.isDefined)
      )
      .flatMap {
        // If traffic control is disabled, cost is 0.
        // Short circuit early to avoid unnecessarily generation a confirmation request / response
        case false =>
          EitherT.pure(
            SubmissionCostEstimation(
              snapshot.timestamp,
              NonNegativeLong.zero,
              NonNegativeLong.zero,
            )
          )
        case true => estimateCost
      }
  }

  /** Estimate the cost of the confirmation request by creating one from the transaction and mocking
    * out the rest of the data
    */
  private def estimateConfirmationRequestCost(
      trafficStateController: TrafficStateController,
      submitterInfo: SubmitterInfo,
      client: SynchronizerSnapshotSyncCryptoApi,
      snapshot: TopologySnapshot,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      keyResolver: LfKeyResolver,
      costHints: CostEstimationHints,
      disclosedContracts: Map[LfContractId, ContractInstance],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, NonNegativeLong] = {
    val lookupContractsWithDisclosed: ContractInstanceOfId =
      TransactionProcessingSteps.lookupContractsWithDisclosed(disclosedContracts, contractStore)

    for {
      // Mock signatures for the actAs parties
      mockedSignatures <- submitterInfo.actAs
        .parTraverse { party =>
          for {
            partyId <- EitherT.fromEither[FutureUnlessShutdown](PartyId.fromLfParty(party))
            signingKeysWithThresholdO <- EitherT
              .liftF[FutureUnlessShutdown, String, Option[SigningKeysWithThreshold]](
                snapshot.signingKeysWithThreshold(partyId)
              )
            // If the party has no signing keys, do not mock signatures.
            // This makes it technically possible to estimate cost also for local parties
            mocked = signingKeysWithThresholdO
              .map(mockSignatures(costHints, _))
              .getOrElse(Seq.empty)
          } yield {
            partyId -> mocked
          }
        }
        .map(_.toMap)
      hasExternalSignatures = mockedSignatures.values.exists(_.nonEmpty)
      submitterInfoWithMockSignatures = {
        // For external parties set the externally signed info with mock data
        Option
          .when(hasExternalSignatures)(mockedSignatures)
          .map { signatures =>
            submitterInfo.copy(
              externallySignedSubmission = Some(
                ExternallySignedSubmission(
                  version = V2,
                  signatures = signatures,
                  transactionUUID = UUID.randomUUID(),
                  mediatorGroup = mediatorGroupIndex,
                  maxRecordTimeO = None,
                )
              )
            )
          }
          .getOrElse(submitterInfo)
          .copy(submissionId =
            Some(LedgerSubmissionId.assertFromString(UUID.randomUUID().toString))
          )
      }
      // Generate a confirmation request
      mockedConfirmationRequest <- confirmationRequestFactory
        .createConfirmationRequest(
          wfTransaction,
          submitterInfoWithMockSignatures,
          // The external signing API does not support workflow IDs
          // For local parties, assume it is set to give a more pessimistic estimation
          Option.when(!hasExternalSignatures)(
            WorkflowId.assertFromString(UUID.randomUUID().toString)
          ),
          keyResolver,
          mediatorGroupRecipient,
          client,
          sessionKeyStore,
          lookupContractsWithDisclosed,
          CantonTimestamp.MaxValue,
          psid.protocolVersion,
        )
        .leftMap(_.toString)
      batch <- EitherT.liftF(mockedConfirmationRequest.asBatch(snapshot))
      estimatedCost <- EitherT.liftF(
        trafficStateController
          // Do not log the cost estimation for this dry run as logs are used to observe real costs
          .computeCost(batch, snapshot, logCost = false)
          // We already short-circuit earlier so the cost should not be empty here but fallback to 0 again anyway
          // when traffic control is disabled
          .map(_.map(_.cost).getOrElse(NonNegativeLong.zero))
      )
    } yield estimatedCost
  }

  /** Estimate the cost of the confirmation response sent by the _executing_ participant node. This
    * is also a good estimation of the cost incurred by the confirming participants of the external
    * party because they also send a confirmation for the root view position. It may not be as
    * accurate for nodes that confirm subviews.
    */
  private def estimateConfirmationResponseCost(
      trafficStateController: TrafficStateController,
      submitterInfo: SubmitterInfo,
      client: SynchronizerSnapshotSyncCryptoApi,
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, NonNegativeLong] =
    for {
      hosting <- EitherT.liftF(snapshot.activeParticipantsOfParties(submitterInfo.actAs))
      confirmationResponse <- EitherT.pure[FutureUnlessShutdown, String](
        ConfirmationResponse.tryCreate(
          // Estimate the cost for a participant that has full visibility of the transaction
          // and will confirm the root view position.
          // This will not be as accurate for participant that only confirm sub views
          Some(ViewPosition.root),
          // Assume it will approve, if it rejects the cost may be higher due to the rejection reason
          LocalApprove(psid.protocolVersion),
          // The executing participant adds its own admin party to the set of confirming parties
          // Additionally add all other hosting participants of the submitting parties that are not this one
          Set(participantId.adminParty.toLf) ++ hosting.collect {
            case (party, hostingParticipants) if hostingParticipants.contains(participantId) =>
              party
          }.toSeq,
        )
      )
      signedConfirmationResponse <- EitherT.liftF {
        // Generate a mock confirmation response
        val confirmationResponses = ConfirmationResponses.tryCreate(
          requestId = RequestId(snapshot.timestamp),
          rootHash = rootHash,
          psid,
          participantId,
          NonEmpty.mk(Seq, confirmationResponse),
          psid.protocolVersion,
        )
        SignedProtocolMessage.trySignAndCreate(confirmationResponses, client)
      }
      batch = Batch.of(
        psid.protocolVersion,
        (signedConfirmationResponse -> Recipients.cc(mediatorGroupRecipient)),
      )
      estimatedCost <- EitherT.liftF(
        trafficStateController
          // Do not log the cost estimation for this dry run as logs are used to observe real costs
          .computeCost(batch, snapshot, logCost = false)
          // We already short-circuit earlier so the cost should not be empty here but fallback to 0 again anyway
          // when traffic control is disabled
          .map(_.map(_.cost).getOrElse(NonNegativeLong.zero))
      )
    } yield estimatedCost

  private def mockSignatures(
      costHints: CostEstimationHints,
      partyAuth: SigningKeysWithThreshold,
  ): Seq[Signature] =
    // If hints are provided, use them to mock a signature with the expected algorithm
    if (costHints.signingAlgorithmSpec.nonEmpty) {
      costHints.signingAlgorithmSpec.map(mockSignature(_, None))
    } else {
      // Otherwise mock threshold-many signatures from the first keys in the party to key mapping
      partyAuth.keys.forgetNE
        .flatMap { key =>
          synchronizerCrypto.pureCrypto.signingAlgorithmSpecs.allowed
            .find(_.supportedSigningKeySpecs.contains(key.keySpec))
            .map(mockSignature(_, Some(key.fingerprint)))
        }
        .take(partyAuth.threshold.value)
        .toSeq
    }
}

object TrafficCostEstimator {
  // Mock values
  // session signing keys is a def to avoid compression effects with identical data
  private def sessionSigningKey = {
    val rawKeyPair = Ed25519Sign.KeyPair.newKeyPair()
    val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
    val publicKey = new SubjectPublicKeyInfo(algoId, rawKeyPair.getPublicKey).getEncoded
    SigningPublicKey
      .create(
        format = CryptoKeyFormat.DerX509Spki,
        key = ByteString.copyFrom(publicKey),
        keySpec = SigningKeySpec.EcCurve25519,
        usage = SigningKeyUsage.ProtocolOnly,
      )
      .valueOr { err =>
        throw new IllegalStateException(s"Invalid mock session signing key: $err")
      }
  }
  private val mediatorGroupIndex = MediatorGroupIndex.zero
  private val mediatorGroupRecipient = MediatorGroupRecipient(mediatorGroupIndex)
  private val rootHash = RootHash(mockHash)

  // A SyncCryptoSigner implementation that returns mock value for Signature
  // Makes a best effort to return an accurately sized signature including depending on whether session keys are enabled
  private class MockCryptoSigner(
      participantId: ParticipantId,
      allowedSigningKeySpecs: NonEmpty[Set[SigningAlgorithmSpec]],
      withSessionKeys: Boolean,
      override protected val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends SyncCryptoSigner {
    override protected def cryptoPrivateStore: CryptoPrivateStore =
      throw new UnsupportedOperationException(
        "Private store should not be used in this mock implementation"
      )
    override def sign(
        topologySnapshot: TopologySnapshot,
        hash: Hash,
        usage: NonEmpty[Set[SigningKeyUsage]],
    )(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] = for {
      signature <- mockParticipantSignature(
        participantId,
        topologySnapshot,
        usage,
        allowedSigningKeySpecs,
      )
      maybeWithSignatureDelegation <- EitherT.fromEither[FutureUnlessShutdown](
        maybeAddSignatureDelegation(signature, topologySnapshot)
      )
    } yield maybeWithSignatureDelegation

    override def close(): Unit = ()

    private def maybeAddSignatureDelegation(
        signature: Signature,
        topologySnapshot: TopologySnapshot,
    ) = if (withSessionKeys) {
      SignatureDelegation
        .create(
          TrafficCostEstimator.sessionSigningKey,
          SignatureDelegationValidityPeriod(
            topologySnapshot.timestamp,
            PositiveFiniteDuration.ofMinutes(5),
          ),
          signature,
        )
        .leftMap(err =>
          SyncCryptoError.SyncCryptoSigningError(InvariantViolation(err)): SyncCryptoError
        )
        .map(signature.addSignatureDelegation)
    } else Right(signature)

    private def mockParticipantSignature(
        participantId: ParticipantId,
        topologySnapshot: TopologySnapshot,
        usage: NonEmpty[Set[SigningKeyUsage]],
        allowedSigningKeySpecs: NonEmpty[Set[SigningAlgorithmSpec]],
    )(implicit
        traceContext: TraceContext,
        executionContext: ExecutionContext,
    ): EitherT[FutureUnlessShutdown, SyncCryptoError, Signature] =
      for {
        participantSigningKeys <- EitherT.liftF(
          topologySnapshot.signingKeys(participantId, usage)
        )
        key <- EitherT.fromOption[FutureUnlessShutdown](
          participantSigningKeys.headOption,
          SyncCryptoError.KeyNotAvailable(
            participantId.member,
            Signing,
            topologySnapshot.timestamp,
            Seq.empty,
          ): SyncCryptoError,
        )
        keySpec = key.keySpec
        signingAlgorithm <- EitherT.fromOption[FutureUnlessShutdown](
          allowedSigningKeySpecs
            .find(_.supportedSigningKeySpecs.contains(keySpec)),
          SyncCryptoError.KeyNotAvailable(
            participantId.member,
            Signing,
            topologySnapshot.timestamp,
            Seq.empty,
          ): SyncCryptoError,
        )
      } yield mockSignature(signingAlgorithm, Some(key.fingerprint))
  }

  private def mockHash = Hash
    .digest(
      HashPurpose.TestHashPurpose,
      ByteString.copyFrom(Random.nextBytes(32)),
      Sha256,
    )

  private def mockSignature(
      spec: SigningAlgorithmSpec,
      signedBy: Option[Fingerprint],
  ) = Signature.create(
    spec.supportedSignatureFormats.head1,
    ByteString.copyFrom(Random.nextBytes(spec.approximateSignatureSize)),
    signedBy.getOrElse(Fingerprint.tryFromString(mockHash.toHexString)),
    Some(spec),
    None,
  )
}
