// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.sequencing.client.SequencerClient
import com.digitalasset.canton.topology.client.SynchronizerTopologyClientWithInit
import com.digitalasset.canton.topology.processing.InitialTopologySnapshotValidator
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.ExecutionContext

trait SynchronizerTopologyInitializationCallback {
  def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

class StoreBasedSynchronizerTopologyInitializationCallback
    extends SynchronizerTopologyInitializationCallback {
  override def callback(
      topologyStoreInitialization: InitialTopologySnapshotValidator,
      topologyClient: SynchronizerTopologyClientWithInit,
      sequencerClient: SequencerClient,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    for {
      topologyTransactions <- sequencerClient
        .downloadTopologyStateForInit(maxRetries = retry.Forever, retryLogLevel = None)
      _ <- topologyStoreInitialization.validateAndApplyInitialTopologySnapshot(topologyTransactions)
      _ <- EitherT.right(topologyClient.initialize())
    } yield ()
}
