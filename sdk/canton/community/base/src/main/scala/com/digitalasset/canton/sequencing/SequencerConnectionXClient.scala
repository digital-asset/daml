// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcError}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.sequencing.protocol.HandshakeResponse
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContextExecutor

/** A generic client to send requests to a sequencer. This trait attempts to be independent of the
  * underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnectionXClient {
  import SequencerConnectionXClient.*

  def getApiName(retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, String]

  def performHandshake(
      clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
      minimumProtocolVersion: Option[ProtocolVersion],
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, HandshakeResponse]

  def getSynchronizerAndSequencerIds(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, (SynchronizerId, SequencerId)]

  def getStaticSynchronizerParameters(
      retryPolicy: GrpcError => Boolean = CantonGrpcUtil.RetryPolicy.noRetry
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerConnectionXClientError, StaticSynchronizerParameters]
}

object SequencerConnectionXClient {
  sealed trait SequencerConnectionXClientError extends Product with Serializable
  object SequencerConnectionXClientError {

    /** An error happened with the underlying transport.
      */
    final case class ConnectionError(error: ConnectionX.ConnectionXError)
        extends SequencerConnectionXClientError

    /** A deserialization error happened when decoding a response.
      */
    final case class DeserializationError(message: String) extends SequencerConnectionXClientError
  }
}

trait SequencerConnectionXClientFactory {
  def create(connection: ConnectionX)(implicit
      ec: ExecutionContextExecutor
  ): SequencerConnectionXClient
}
