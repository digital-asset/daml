// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SequencerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContextExecutor

/** A generic stub to interact with a sequencer. This trait attempts to be independent of the
  * underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnectionXStub {
  import SequencerConnectionXStub.*

  def getApiName(retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, String]

  def performHandshake(
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, HandshakeResponse]

  def getSynchronizerAndSequencerIds(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SequencerConnectionXStubError,
    (PhysicalSynchronizerId, SequencerId),
  ]

  def getStaticSynchronizerParameters(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXStubError, StaticSynchronizerParameters]
}

object SequencerConnectionXStub {
  sealed trait SequencerConnectionXStubError extends Product with Serializable
  object SequencerConnectionXStubError {

    /** An error happened with the underlying connection.
      */
    final case class ConnectionError(error: ConnectionX.ConnectionXError)
        extends SequencerConnectionXStubError

    /** A deserialization error happened when decoding a response.
      */
    final case class DeserializationError(message: String) extends SequencerConnectionXStubError
  }
}

trait SequencerConnectionXStubFactory {
  def createStub(connection: ConnectionX)(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionXStub

  def createUserStub(connection: ConnectionX, clientAuth: GrpcSequencerClientAuth)(implicit
      ec: ExecutionContextExecutor
  ): UserSequencerConnectionXStub
}
