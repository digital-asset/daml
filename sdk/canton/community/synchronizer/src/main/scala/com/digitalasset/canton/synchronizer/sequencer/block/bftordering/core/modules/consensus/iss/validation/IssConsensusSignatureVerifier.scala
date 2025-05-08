// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopologyInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
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
final class IssConsensusSignatureVerifier[E <: Env[E]] {

  private type VerificationResult =
    E#FutureUnlessShutdownT[Either[Seq[SignatureCheckError], Unit]]

  def verify(
      message: SignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage],
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
  ): VerificationResult =
    validateSignedMessage[ConsensusSegment.ConsensusMessage.PbftNetworkMessage](
      validateMessage(_, topologyInfo)
    )(message)(
      context,
      topologyInfo.currentCryptoProvider,
      traceContext,
    )

  private def validateMessage(
      message: ConsensusSegment.ConsensusMessage.PbftNetworkMessage,
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
  ): VerificationResult = {
    implicit val implicitCryptoProvider: CryptoProvider[E] = topologyInfo.currentCryptoProvider

    message match {
      case p: PrePrepare =>
        validatePrePrepare(p, topologyInfo)
      case Prepare(
            blockMetadata,
            viewNumber,
            hash,
            from,
          ) =>
        context.pureFuture(Either.unit[Seq[SignatureCheckError]])
      case _: ConsensusSegment.ConsensusMessage.Commit =>
        context.pureFuture(Either.unit[Seq[SignatureCheckError]])
      case msg: ConsensusSegment.ConsensusMessage.ViewChange =>
        validateViewChange(msg, topologyInfo)
      case ConsensusSegment.ConsensusMessage.NewView(
            blockMetadata,
            segmentIndex,
            viewNumber,
            viewChanges,
            prePrepares,
            from,
          ) =>
        collectFuturesAndFlatten(
          viewChanges.map(validateSignedMessage(validateViewChange(_, topologyInfo))) ++
            prePrepares.map(validateSignedMessage(validatePrePrepare(_, topologyInfo)))
        )
    }
  }

  private def validateProofOfAvailability(
      proofOfAvailability: ProofOfAvailability
  )(implicit
      context: FutureContext[E],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): VerificationResult = collectFutures[SignatureCheckError] {
    proofOfAvailability.acks.map { ack =>
      val hash = AvailabilityAck.hashFor(
        proofOfAvailability.batchId,
        proofOfAvailability.epochNumber,
        ack.from,
      )
      cryptoProvider.verifySignature(hash, ack.from, ack.signature)
    }
  }

  private def validateOrderingBlock(
      block: OrderingBlock
  )(implicit
      context: FutureContext[E],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): VerificationResult =
    collectFuturesAndFlatten(block.proofs.map(validateProofOfAvailability(_)))

  private def validatePrePrepare(
      message: PrePrepare,
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
  ): VerificationResult = message match {
    case PrePrepare(
          blockMetadata,
          viewNumber,
          block,
          canonicalCommitSet,
          from,
        ) =>
      implicit val cryptoProvider: CryptoProvider[E] = topologyInfo.currentCryptoProvider
      // Canonical commit sets are validated in more detail later in the process
      val maybeCanonicalCommitSetEpochNumber =
        canonicalCommitSet.sortedCommits.map(_.message.blockMetadata.epochNumber).headOption
      val prePrepareEpochNumber = blockMetadata.epochNumber
      val cryptoProviderForCanonicalCommits =
        if (maybeCanonicalCommitSetEpochNumber.contains(prePrepareEpochNumber)) {
          topologyInfo.currentCryptoProvider
        } else {
          topologyInfo.previousCryptoProvider
        }
      collectFuturesAndFlatten(
        canonicalCommitSet.sortedCommits.map(
          validateMessageSignature(_, cryptoProviderForCanonicalCommits)
        ) :+ validateOrderingBlock(block)
      )
  }

  private def validateMessageSignature[T <: PbftNetworkMessage](
      signedMessage: SignedMessage[T],
      cryptoProvider: CryptoProvider[E],
  )(implicit context: FutureContext[E], traceContext: TraceContext): VerificationResult = {
    implicit val cp: CryptoProvider[E] = cryptoProvider
    validateSignedMessage((_: PbftNetworkMessage) =>
      context.pureFuture(Either.unit[Seq[SignatureCheckError]])
    )(signedMessage)
  }

  private def validateViewChange(
      message: ConsensusSegment.ConsensusMessage.ViewChange,
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
  ): VerificationResult = message match {
    case ConsensusSegment.ConsensusMessage.ViewChange(
          _blockMetadata,
          _segmentIndex,
          _viewNumber,
          certs,
          _from,
        ) =>
      collectFuturesAndFlatten(certs.map(validateConsensusCertificate(_, topologyInfo)))
  }

  def validateConsensusCertificate(
      certificate: ConsensusCertificate,
      topologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: FutureContext[E],
      traceContext: TraceContext,
  ): VerificationResult = {
    implicit val implicitCryptoProvider: CryptoProvider[E] = topologyInfo.currentCryptoProvider
    val prePrepareValidationF =
      validateSignedMessage[PrePrepare](validatePrePrepare(_, topologyInfo))(certificate.prePrepare)
    val reminderValidationF: VerificationResult = certificate match {
      case CommitCertificate(_, commits) =>
        collectFuturesAndFlatten(
          commits.map(validateMessageSignature(_, topologyInfo.currentCryptoProvider))
        )
      case PrepareCertificate(_, prepares) =>
        collectFuturesAndFlatten(
          prepares.map(validateMessageSignature(_, topologyInfo.currentCryptoProvider))
        )
    }
    collectFuturesAndFlatten(Seq(prePrepareValidationF, reminderValidationF))
  }

  private def validateSignedMessage[A <: PbftNetworkMessage](
      validator: A => VerificationResult
  )(signedMessage: SignedMessage[A])(implicit
      context: FutureContext[E],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): VerificationResult =
    context.mapFuture(
      context.zipFuture(
        validator(signedMessage.message),
        collectFutures(
          Seq(
            cryptoProvider.verifySignedMessage(
              signedMessage,
              AuthenticatedMessageType.BftSignedConsensusMessage,
            )
          )
        ),
      )
    )(
      PureFun.Util.CollectPairErrors[SignatureCheckError]()
    )

  private def collectFutures[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Err, Unit]]]
  )(implicit
      context: FutureContext[E]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(context.sequenceFuture(futures))(PureFun.Util.CollectErrors())

  private def collectFuturesAndFlatten[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Seq[Err], Unit]]]
  )(implicit
      context: FutureContext[E]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(collectFutures(futures))(PureFun.Either.LeftMap(PureFun.Seq.Flatten()))
}
