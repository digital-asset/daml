// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.sequencer

import cats.data.EitherT
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.SequencerConnectClient.{
  Error,
  SynchronizerClientBootstrapInfo,
}
import com.digitalasset.canton.common.sequencer.grpc.GrpcSequencerConnectClient
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnection}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.{Member, ParticipantId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}

import scala.concurrent.ExecutionContextExecutor

trait SequencerConnectClient extends NamedLogging with AutoCloseable {

  def getSynchronizerClientBootstrapInfo(synchronizerAlias: SynchronizerAlias)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, SynchronizerClientBootstrapInfo]

  /** @param synchronizerIdentifier Used for logging purpose
    */
  def getSynchronizerParameters(synchronizerIdentifier: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, StaticSynchronizerParameters]

  /** @param synchronizerIdentifier Used for logging purpose
    */
  def getSynchronizerId(synchronizerIdentifier: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, SynchronizerId]

  def handshake(
      synchronizerAlias: SynchronizerAlias,
      request: HandshakeRequest,
      dontWarnOnDeprecatedPV: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, HandshakeResponse]

  def isActive(
      participantId: ParticipantId,
      synchronizerAlias: SynchronizerAlias,
      waitForActive: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Error, Boolean]

  protected def handleVerifyActiveResponse(
      response: v30.SequencerConnect.VerifyActiveResponse
  ): Either[Error, Boolean] = response.value match {
    case v30.SequencerConnect.VerifyActiveResponse.Value.Success(success) =>
      Right(success.isActive)
    case v30.SequencerConnect.VerifyActiveResponse.Value.Failure(failure) =>
      Left(Error.DeserializationFailure(failure.reason))
    case v30.SequencerConnect.VerifyActiveResponse.Value.Empty =>
      Left(Error.InvalidResponse("Missing response from VerifyActive"))
  }

  def registerOnboardingTopologyTransactions(
      synchronizerAlias: SynchronizerAlias,
      member: Member,
      topologyTransactions: Seq[GenericSignedTopologyTransaction],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Error, Unit]
}

object SequencerConnectClient {

  type Builder = (SynchronizerAlias, SequencerConnection) => SequencerConnectClient

  sealed trait Error {
    def message: String
  }
  object Error {
    final case class DeserializationFailure(err: String) extends Error {
      def message: String = s"Unable to deserialize proto: $err"
    }
    final case class InvalidState(message: String) extends Error
    final case class InvalidResponse(message: String) extends Error
    final case class Transport(message: String) extends Error
  }

  def apply(
      synchronizerAlias: SynchronizerAlias,
      sequencerConnection: SequencerConnection,
      timeouts: ProcessingTimeout,
      traceContextPropagation: TracingConfig.Propagation,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectClient =
    sequencerConnection match {
      case connection: GrpcSequencerConnection =>
        new GrpcSequencerConnectClient(
          connection,
          timeouts,
          traceContextPropagation,
          SequencerClient
            .loggerFactoryWithSequencerAlias(
              loggerFactory.append("synchronizerAlias", synchronizerAlias.toString),
              connection.sequencerAlias,
            )
            .append("sequencerConnection", connection.endpoints.map(_.toString).mkString(",")),
        )
    }

  final case class SynchronizerClientBootstrapInfo(
      synchronizerId: SynchronizerId,
      sequencerId: SequencerId,
  )
}
