// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferMessageValidator.StateTransferValidationResult.{
  DropResult,
  InvalidResult,
  ValidResult,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.{
  ConsensusCertificateValidator,
  IssConsensusSignatureVerifier,
}
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

  def validateUnverifiedStateTransferNetworkMessage(
      msg: SignedMessage[StateTransferMessage.StateTransferNetworkMessage],
      latestLocallyCompletedEpoch: EpochNumber,
      orderingTopologyInfo: OrderingTopologyInfo[E],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): StateTransferMessageResult = {

    val validationResult =
      msg.message match {
        case request: StateTransferMessage.BlockTransferRequest =>
          validateBlockTransferRequest(request) match {
            case Left(error) =>
              val reqError = s"Invalid block transfer request: $error, dropping..."
              InvalidResult(reqError, request.from)
            case Right(()) => ValidResult
          }
        case response: StateTransferMessage.BlockTransferResponse =>
          response.commitCertificate match {
            case Some(cc)
                if cc.prePrepare.message.blockMetadata.epochNumber <= latestLocallyCompletedEpoch =>
              val respReason =
                s"Old block transfer response from epoch ${cc.prePrepare.message.blockMetadata.epochNumber}, " +
                  s"we have completed epoch $latestLocallyCompletedEpoch, dropping..."
              DropResult(respReason)
            case _ =>
              validateBlockTransferResponse(
                response,
                latestLocallyCompletedEpoch,
                orderingTopologyInfo.currentMembership,
              ) match {
                case Left(error) =>
                  val respError = s"Invalid block transfer response: $error, dropping..."
                  InvalidResult(respError, response.from)
                case Right(()) => ValidResult
              }
          }
      }

    validationResult match {
      case ValidResult =>
        verifyStateTransferMessage(
          msg,
          orderingTopologyInfo.currentMembership,
          orderingTopologyInfo.currentCryptoProvider,
        )
      case InvalidResult(error, from) =>
        logger.warn(error)
        emitNonCompliance(metrics)(
          from,
          metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
        )
      case DropResult(reason) =>
        logger.info(reason)
    }

    StateTransferMessageResult.Continue
  }

  // Note: we skip explicitly checking that `request.from` is a peer in the active topology.
  // The next step (verify) first checks that the source is active in the topology
  // before trying to verify the provided cryptographic signature(s)
  def validateBlockTransferRequest(
      request: BlockTransferRequest
  ): Either[String, Unit] =
    for {
      _ <- Either.cond(
        request.epoch > Genesis.GenesisEpochNumber,
        (),
        s"state transfer is supported only after genesis, but start epoch ${request.epoch} received",
      )
    } yield ()

  // Note: we don't need to verify that `response.from` is a peer in the active topology.
  // The BlockTransferResponse message is designed such that the sender of it can be
  // from a future topology. However, the contained CommitCertificates must be from peers
  // in the active topology, and this is already checked in the next step (verify).
  def validateBlockTransferResponse(
      response: BlockTransferResponse,
      latestLocallyCompletedEpoch: EpochNumber,
      membership: Membership,
  ): Either[String, Unit] = {
    val from = response.from
    val commitCert = response.commitCertificate
    val currentEpoch = latestLocallyCompletedEpoch + 1
    lazy val consensusCertificateValidator = new ConsensusCertificateValidator(
      membership.orderingTopology.strongQuorum
    )
    for {
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
      signatureVerifier.verifyConsensusCertificate(commitCertificate, orderingTopologyInfo)
    ) {
      case Success(Right(())) =>
        Some(
          StateTransferMessage.BlockVerified(commitCertificate, from)
        )
      case Success(Left(errors)) =>
        val blockMetadata = commitCertificate.prePrepare.message.blockMetadata
        logger.warn(
          s"Commit certificate ($blockMetadata) from '$from' failed signature verification, dropping: $errors"
        )
        emitNonCompliance(metrics)(
          from,
          metrics.security.noncompliant.labels.violationType.values.StateTransferInvalidMessage,
        )
        None
      case Failure(exception) =>
        logger.warn(s"Commit certificate from '$from' could not be verified, dropping", exception)
        None
    }
}

object StateTransferMessageValidator {
  sealed trait StateTransferValidationResult extends Product with Serializable
  object StateTransferValidationResult {
    case object ValidResult extends StateTransferValidationResult
    final case class InvalidResult(
        error: String,
        from: BftNodeId,
    ) extends StateTransferValidationResult
    final case class DropResult(
        reason: String
    ) extends StateTransferValidationResult
  }
}
