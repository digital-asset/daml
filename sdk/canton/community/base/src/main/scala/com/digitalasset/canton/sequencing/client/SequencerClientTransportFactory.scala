// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil
import org.apache.pekko.stream.Materializer

import scala.concurrent.*

trait SequencerClientTransportFactory {

  def makeTransport(
      sequencerConnections: SequencerConnections,
      member: Member,
      requestSigner: RequestSigner,
      allowReplay: Boolean = true,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[
    NonEmpty[Map[SequencerAlias, SequencerClientTransport & SequencerClientTransportPekko]]
  ] =
    for {
      connections <- MonadUtil.sequentialTraverse(sequencerConnections.connections.forgetNE)(conn =>
        makeTransport(conn, member, requestSigner, allowReplay)
          .map(transport => conn.sequencerAlias -> transport)
      )
    } yield NonEmptyUtil.fromUnsafe(connections.toMap)

  def makeTransport(
      connection: SequencerConnection,
      member: Member,
      requestSigner: RequestSigner,
      allowReplay: Boolean,
  )(implicit
      executionContext: ExecutionContextExecutor,
      executionSequencerFactory: ExecutionSequencerFactory,
      materializer: Materializer,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SequencerClientTransport & SequencerClientTransportPekko]

}
