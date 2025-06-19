// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.{
  RetransmissionRequest,
  RetransmissionResponse,
  RetransmissionsNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  FutureContext,
  PureFun,
}
import com.digitalasset.canton.tracing.TraceContext

/** If the verifier approves a message then the message is from whom it says it is, recursively
  * (e.g., commit certificates contain commit messages that are from whom they say they are).
  */
final class IssConsensusSignatureVerifier[E <: Env[E]](metrics: BftOrderingMetrics) {

  private type VerificationResult =
    E#FutureUnlessShutdownT[Either[Seq[SignatureCheckError], Unit]]

  def verify(
      message: SignedMessage[PbftNetworkMessage],
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult = {
    implicit val implicitTopologyInfo: OrderingTopologyInfo[E] = topologyInfo
    validateSignedMessage[PbftNetworkMessage](
      validateMessage(_)
    )(message)
  }

  private def validateMessage(message: PbftNetworkMessage)(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
      topologyInfo: OrderingTopologyInfo[E],
  ): VerificationResult =
    message match {
      case p: PrePrepare =>
        validatePrePrepare(p)
      case _: Prepare | _: Commit =>
        context.pureFuture(Either.unit[Seq[SignatureCheckError]])
      case msg: ViewChange =>
        validateViewChange(msg)
      case nv: NewView =>
        collectFuturesAndFlatten(
          nv.viewChanges.map(validateSignedMessage(validateViewChange(_))) ++
            nv.prePrepares.map(validateSignedMessage(validatePrePrepare(_))),
          orderingStage = "consensus-validate-new-view",
        )
    }

  private def validateProofOfAvailability(
      proofOfAvailability: ProofOfAvailability
  )(implicit
      context: FutureContext[E],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult =
    collectFutures[SignatureCheckError](
      proofOfAvailability.acks.map { ack =>
        val hash = AvailabilityAck.hashFor(
          proofOfAvailability.batchId,
          proofOfAvailability.epochNumber,
          ack.from,
          metrics,
        )
        cryptoProvider.verifySignature(
          hash,
          ack.from,
          ack.signature,
          "consensus-signature-verify-poa-ack",
        )
      },
      orderingStage = "consensus-validate-poa",
    )

  private def validateOrderingBlock(
      block: OrderingBlock
  )(implicit
      context: FutureContext[E],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult =
    collectFuturesAndFlatten(
      block.proofs.map(validateProofOfAvailability(_)),
      orderingStage = "consensus-validate-ordering-block",
    )

  private def validatePrePrepare(
      message: PrePrepare
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
      topologyInfo: OrderingTopologyInfo[E],
  ): VerificationResult = message match {
    case PrePrepare(
          blockMetadata,
          _,
          block,
          canonicalCommitSet,
          _,
        ) =>
      implicit val cryptoProvider: CryptoProvider[E] = topologyInfo.currentCryptoProvider
      // Canonical commit sets are validated in more detail later in the process
      val maybeCanonicalCommitSetEpochNumber =
        canonicalCommitSet.sortedCommits.map(_.message.blockMetadata.epochNumber).headOption
      val prePrepareEpochNumber = blockMetadata.epochNumber
      val (cryptoProviderForCanonicalCommits, membership) =
        if (maybeCanonicalCommitSetEpochNumber.contains(prePrepareEpochNumber)) {
          (topologyInfo.currentCryptoProvider, topologyInfo.currentMembership)
        } else {
          (topologyInfo.previousCryptoProvider, topologyInfo.previousMembership)
        }
      collectFuturesAndFlatten(
        canonicalCommitSet.sortedCommits.map(commit =>
          validateSignedMessage(
            (_: PbftNetworkMessage) => context.pureFuture(Either.unit[Seq[SignatureCheckError]]),
            cryptoProviderForCanonicalCommits,
            membership,
            AuthenticatedMessageType.BftSignedConsensusMessage,
          )(commit)
        ) :+ validateOrderingBlock(block),
        orderingStage = "consensus-validate-pre-prepare",
      )
  }

  private def validateViewChange(
      message: ViewChange
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
      topologyInfo: OrderingTopologyInfo[E],
  ): VerificationResult =
    collectFuturesAndFlatten(
      message.consensusCerts.map(verifyConsensusCertificate(_, topologyInfo)),
      orderingStage = "consensus-validate-view-change",
    )

  def verifyRetransmissionMessage(
      message: SignedMessage[RetransmissionsNetworkMessage],
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult = {
    def validateMessage(message: RetransmissionsNetworkMessage): VerificationResult =
      message match {
        case _: RetransmissionRequest => context.pureFuture(Either.unit[Seq[SignatureCheckError]])
        case RetransmissionResponse(_, consensusCerts) =>
          collectFuturesAndFlatten(
            consensusCerts.map(verifyConsensusCertificate(_, topologyInfo)),
            orderingStage = "consensus-validate-retransmission-response",
          )
      }
    validateSignedMessage[RetransmissionsNetworkMessage](
      validateMessage,
      topologyInfo.currentCryptoProvider,
      topologyInfo.currentMembership,
      AuthenticatedMessageType.BftSignedRetransmissionMessage,
    )(message)
  }

  def verifyConsensusCertificate(
      certificate: ConsensusCertificate,
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult = {
    implicit val topologyImplicit: OrderingTopologyInfo[E] = topologyInfo
    def validate[T <: PbftNetworkMessage](signedMessage: SignedMessage[T]): VerificationResult =
      validateSignedMessage((_: PbftNetworkMessage) =>
        context.pureFuture(Either.unit[Seq[SignatureCheckError]])
      )(signedMessage)
    val prePrepareValidationF =
      validateSignedMessage[PrePrepare](validatePrePrepare(_))(certificate.prePrepare)
    val remainingValidationF: VerificationResult = certificate match {
      case CommitCertificate(_, commits) =>
        collectFuturesAndFlatten(
          commits.map(validate(_)),
          orderingStage = "consensus-validate-commit-certificate",
        )
      case PrepareCertificate(_, prepares) =>
        collectFuturesAndFlatten(
          prepares.map(validate(_)),
          orderingStage = "consensus-validate-prepare-certificate",
        )
    }
    collectFuturesAndFlatten(
      Seq(prePrepareValidationF, remainingValidationF),
      orderingStage = "consensus-validate-consensus-certificate",
    )
  }

  private def validateSignedMessage[A <: PbftNetworkMessage](
      validator: A => VerificationResult
  )(signedMessage: SignedMessage[A])(implicit
      context: FutureContext[E],
      topologyInfo: OrderingTopologyInfo[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult =
    validateSignedMessage(
      validator,
      topologyInfo.currentCryptoProvider,
      topologyInfo.currentMembership,
      AuthenticatedMessageType.BftSignedConsensusMessage,
    )(signedMessage)

  private def validateSignedMessage[A <: ProtocolVersionedMemoizedEvidence & MessageFrom](
      validator: A => VerificationResult,
      cryptoProvider: CryptoProvider[E],
      membership: Membership,
      authenticatedMessageType: AuthenticatedMessageType,
  )(signedMessage: SignedMessage[A])(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): VerificationResult =
    if (membership.orderingTopology.contains(signedMessage.from))
      context.mapFuture(
        context.zipFuture(
          validator(signedMessage.message),
          collectFutures(
            Seq(
              cryptoProvider.verifySignedMessage(
                signedMessage,
                authenticatedMessageType,
              )
            ),
            orderingStage = "consensus-verify-signed-message",
          ),
          orderingStage = Some("consensus-validate-signed-message"),
        )
      )(
        PureFun.Util.CollectPairErrors[SignatureCheckError]()
      )
    else {
      val error = SignatureCheckError.SignerHasNoValidKeys(
        s"Cannot verify signature from node ${signedMessage.from}, because it is not currently a valid member"
      )
      context.pureFuture[Either[Seq[SignatureCheckError], Unit]](Left(Seq(error)))
    }

  private def collectFutures[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Err, Unit]]],
      orderingStage: String,
  )(implicit
      context: FutureContext[E]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(context.sequenceFuture(futures, Some(orderingStage)))(
      PureFun.Util.CollectErrors()
    )

  private def collectFuturesAndFlatten[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Seq[Err], Unit]]],
      orderingStage: String,
  )(implicit
      context: FutureContext[E]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(collectFutures(futures, orderingStage))(
      PureFun.Either.LeftMap(PureFun.Seq.Flatten())
    )
}
