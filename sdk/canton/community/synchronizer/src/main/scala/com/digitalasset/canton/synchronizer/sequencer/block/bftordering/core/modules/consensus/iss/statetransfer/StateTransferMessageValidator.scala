// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.{
  ConsensusCertificateValidator,
  IssConsensusSignatureVerifier,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopologyInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferRequest,
  BlockTransferResponse,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.util.{Failure, Success}

final class StateTransferMessageValidator[E <: Env[E]](
    metrics: BftOrderingMetrics,
    override val loggerFactory: NamedLoggerFactory,
)(implicit mc: MetricsContext)
    extends NamedLogging {

  private val signatureVerifier = new IssConsensusSignatureVerifier[E](metrics)

  def validateBlockTransferRequest(
      request: BlockTransferRequest,
      activeMembership: Membership,
  ): Either[String, Unit] = {
    val from = request.from
    val nodes = activeMembership.sortedNodes

    for {
      _ <- Either.cond(
        nodes.contains(from),
        (),
        s"'$from' is requesting state transfer while not being active, active nodes are: $nodes",
      )
      _ <- Either.cond(
        request.epoch > Genesis.GenesisEpochNumber,
        (),
        s"state transfer is supported only after genesis, but start epoch ${request.epoch} received",
      )
    } yield ()
  }

  def validateBlockTransferResponse(
      response: BlockTransferResponse,
      latestLocallyCompletedEpoch: EpochNumber,
      membership: Membership,
  ): Either[String, Unit] = {
    val from = response.from
    val nodes = membership.sortedNodes
    val commitCert = response.commitCertificate
    val currentEpoch = latestLocallyCompletedEpoch + 1
    lazy val consensusCertificateValidator = new ConsensusCertificateValidator(
      membership.orderingTopology.strongQuorum
    )
    for {
      _ <- Either.cond(
        nodes.contains(from),
        (),
        s"received a block transfer response from '$from' which has not been active, active nodes: $nodes",
      )
      _ <- Either.cond(
        commitCert.forall(_.prePrepare.message.blockMetadata.epochNumber == currentEpoch),
        (), {
          val unexpectedEpoch = commitCert.map(_.prePrepare.message.blockMetadata.epochNumber)
          s"received a block transfer response from '$from' containing a pre-prepare with unexpected epoch " +
            s"$unexpectedEpoch, expected $currentEpoch"
        },
      )
      _ <- commitCert
        .map(cert => consensusCertificateValidator.validateConsensusCertificate(cert))
        .fold[Either[String, Unit]](Right(())) { validationResult =>
          validationResult.leftMap(error =>
            s"received a block transfer response from '$from' containing a commit certificate with the following issue: $error"
          )
        }
    } yield ()
  }

  def verifyStateTransferMessage(
      unverifiedMessage: SignedMessage[Consensus.StateTransferMessage.StateTransferNetworkMessage],
      activeMembership: Membership,
      activeCryptoProvider: CryptoProvider[E],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    unverifiedMessage.message match {
      case response: BlockTransferResponse =>
        // Block transfer responses are signed for uniformity/simplicity. However, it is just a thin wrapper around
        //  commit certificates, which themselves contain signed data that is then verified. As long as there's no other
        //  data than commit certs included in the responses, the signature verification can be safely skipped.
        //  As a result, any node can help with state transfer (as long as it provides valid commit certs), even when
        //  its responses are signed with a new/rotated key.
        context.self.asyncSend(
          Consensus.StateTransferMessage.VerifiedStateTransferMessage(response)
        )
      case request: BlockTransferRequest =>
        val from = unverifiedMessage.from
        if (activeMembership.orderingTopology.nodes.contains(from)) {
          context.pipeToSelf(
            activeCryptoProvider
              .verifySignedMessage(
                unverifiedMessage,
                AuthenticatedMessageType.BftSignedStateTransferMessage,
              )
          ) {
            case Failure(exception) =>
              logger.error(
                s"Block transfer request $request from $from could not be verified, dropping",
                exception,
              )
              None
            case Success(Left(errors)) =>
              logger.warn(
                s"Block transfer request $request from $from failed verified, dropping: $errors"
              )
              emitNonCompliance(metrics)(
                from,
                epoch = None,
                view = None,
                block = None,
                metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
              )
              None
            case Success(Right(())) =>
              Some(Consensus.StateTransferMessage.VerifiedStateTransferMessage(request))
          }
        } else {
          logger.info(
            s"Got block transfer request from $from which is not in active membership, dropping"
          )
        }
    }

  def verifyCommitCertificateSignatures(
      commitCertificate: CommitCertificate,
      from: BftNodeId,
      orderingTopologyInfo: OrderingTopologyInfo[E],
  )(implicit context: E#ActorContextT[Consensus.Message[E]], traceContext: TraceContext): Unit =
    context.pipeToSelf(
      signatureVerifier.validateConsensusCertificate(commitCertificate, orderingTopologyInfo)
    ) {
      case Success(Right(())) =>
        Some(
          StateTransferMessage.BlockVerified(commitCertificate, from)
        )
      case Success(Left(errors)) =>
        val blockMetadata = commitCertificate.prePrepare.message.blockMetadata
        logger.warn(
          s"State transfer: commit certificate from '$from' failed signature verification, dropping: $errors"
        )
        emitNonCompliance(metrics)(
          from,
          Some(blockMetadata.epochNumber),
          view = None,
          Some(blockMetadata.blockNumber),
          metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
        )
        None
      case Failure(exception) =>
        logger.warn(
          s"State transfer: commit certificate from '$from' could not be verified, dropping",
          exception,
        )
        None
    }
}
