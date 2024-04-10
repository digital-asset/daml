// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.data.EitherT
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import org.apache.pekko.stream.Materializer

import scala.concurrent.*

trait SequencerClientTransportFactory {

  def makeTransport(
      sequencerConnections: SequencerConnections,
      member: Member,
      requestSigner: RequestSigner,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, NonEmpty[
    Map[SequencerAlias, SequencerClientTransport & SequencerClientTransportPekko]
  ]] =
    MonadUtil
      .sequentialTraverse(sequencerConnections.connections)(conn =>
        makeTransport(conn, member, requestSigner)
          .map(transport => conn.sequencerAlias -> transport)
      )
      .map(transports => NonEmptyUtil.fromUnsafe(transports.toMap))

  def makeTransport(
      connection: SequencerConnection,
      member: Member,
      requestSigner: RequestSigner,
      allowReplay: Boolean = true,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): EitherT[Future, String, SequencerClientTransport & SequencerClientTransportPekko]

}
