// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.grpc

import cats.syntax.option.*
import com.digitalasset.canton.config.KeepAliveClientConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.tracing.TracingConfig.Propagation
import io.grpc.ManagedChannel

import java.util.concurrent.Executor

/** Create a GRPC channel to use for the sequencer client and sequencer administrative operations */
object GrpcSequencerChannelBuilder {
  def apply(
      clientChannelBuilder: ClientChannelBuilder,
      connection: GrpcSequencerConnection,
      maxRequestSize: NonNegativeInt,
      traceContextPropagation: Propagation,
      keepAlive: Option[KeepAliveClientConfig] = Some(KeepAliveClientConfig()),
  )(implicit executor: Executor): ManagedChannel =
    clientChannelBuilder
      .create(
        connection.endpoints,
        connection.transportSecurity,
        executor,
        connection.customTrustCertificates,
        traceContextPropagation,
        maxRequestSize.some,
        keepAlive,
      )
      .build()
}
