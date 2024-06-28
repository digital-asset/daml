// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerAlias
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.transports.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
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
  ): NonEmpty[Map[SequencerAlias, SequencerClientTransport & SequencerClientTransportPekko]] = {
    sequencerConnections.connections.map { conn =>
      conn.sequencerAlias -> makeTransport(conn, member, requestSigner)
    }.toMap
  }

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
  ): SequencerClientTransport & SequencerClientTransportPekko

}
