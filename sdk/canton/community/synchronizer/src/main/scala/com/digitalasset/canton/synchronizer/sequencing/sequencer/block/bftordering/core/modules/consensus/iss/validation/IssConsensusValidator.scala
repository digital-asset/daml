// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.validation

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.ConsensusCertificate
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusSegment,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  Env,
  PureFun,
}
import com.digitalasset.canton.tracing.TraceContext

final class IssConsensusValidator[E <: Env[E]] {

  def validate(
      message: ConsensusSegment.ConsensusMessage.PbftNetworkMessage,
      cryptoProvider: CryptoProvider[E],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): E#FutureUnlessShutdownT[Either[Seq[SignatureCheckError], Unit]] = {
    implicit val implicitCryptoProvider: CryptoProvider[E] = cryptoProvider

    message match {
      case p: ConsensusSegment.ConsensusMessage.PrePrepare =>
        validatePrePrepare(p)
      case ConsensusSegment.ConsensusMessage.Prepare(
            blockMetadata,
            viewNumber,
            hash,
            localTimestamp,
            from,
          ) =>
        context.pureFuture(Either.unit)
      case ConsensusSegment.ConsensusMessage.Commit(
            blockMetadata,
            viewNumber,
            hash,
            localTimestamp,
            from,
          ) =>
        context.pureFuture(Either.unit)
      case msg: ConsensusSegment.ConsensusMessage.ViewChange =>
        validateViewChange(msg)
      case ConsensusSegment.ConsensusMessage.NewView(
            blockMetadata,
            segmentIndex,
            viewNumber,
            localTimestamp,
            viewChanges,
            prePrepares,
            from,
          ) =>
        collectFuturesAndFlatten(prePrepares.map(validateSignedMessage(validatePrePrepare)))
    }
  }

  private type Return = E#FutureUnlessShutdownT[Either[Seq[SignatureCheckError], Unit]]

  private def validateProofOfAvailability(
      proofOfAvailability: ProofOfAvailability
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): Return = collectFutures[SignatureCheckError] {
    proofOfAvailability.acks.map { ack =>
      val hash = AvailabilityAck.hashFor(proofOfAvailability.batchId, ack.from)
      cryptoProvider.verifySignature(hash, ack.from, ack.signature)
    }
  }

  private def validateOrderingBlock(
      block: OrderingBlock
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): Return = collectFuturesAndFlatten(block.proofs.map(validateProofOfAvailability(_)))

  private def validatePrePrepare(
      message: ConsensusSegment.ConsensusMessage.PrePrepare
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): Return = message match {
    case ConsensusSegment.ConsensusMessage.PrePrepare(
          blockMetadata,
          viewNumber,
          localTimestamp,
          block,
          canonicalCommitSet,
          from,
        ) =>
      validateOrderingBlock(block)
  }

  private def validateConsensusCertificate(
      message: ConsensusCertificate
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): Return =
    validateSignedMessage(validatePrePrepare)(message.prePrepare)

  private def validateViewChange(
      message: ConsensusSegment.ConsensusMessage.ViewChange
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      cryptoProvider: CryptoProvider[E],
      traceContext: TraceContext,
  ): Return = message match {
    case ConsensusSegment.ConsensusMessage.ViewChange(
          blockMetadata,
          segmentIndex,
          viewNumber,
          localTimestamp,
          certs,
          from,
        ) =>
      collectFuturesAndFlatten(certs.map(validateConsensusCertificate(_)))
  }

  private def validateSignedMessage[A <: PbftNetworkMessage](
      validator: A => Return
  )(signedMessage: SignedMessage[A]): Return =
    // TODO(#20458) actually validate the signature
    validator(signedMessage.message)

  private def collectFutures[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Err, Unit]]]
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(context.sequenceFuture(futures))(PureFun.Util.CollectErrors())

  private def collectFuturesAndFlatten[Err](
      futures: Seq[E#FutureUnlessShutdownT[Either[Seq[Err], Unit]]]
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]]
  ): E#FutureUnlessShutdownT[Either[Seq[Err], Unit]] =
    context.mapFuture(collectFutures(futures))(PureFun.Either.LeftMap(PureFun.Seq.Flatten()))
}
