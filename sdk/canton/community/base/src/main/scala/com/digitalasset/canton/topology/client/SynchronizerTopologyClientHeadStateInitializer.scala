// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.client

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.topology.processing.SequencedTime
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Responsible for calling [[SynchronizerTopologyClientWithInit.updateHead]] for the first time. */
trait SynchronizerTopologyClientHeadStateInitializer {

  def initialize(
      client: SynchronizerTopologyClientWithInit
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit]
}

/** A topology client head initializer implementation relying solely on maximum timestamps from the
  * topology store.
  */
final class DefaultHeadStateInitializer(store: TopologyStore[TopologyStoreId.SynchronizerStore])
    extends SynchronizerTopologyClientHeadStateInitializer {

  override def initialize(client: SynchronizerTopologyClientWithInit)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[SynchronizerTopologyClientWithInit] =
    store
      .maxTimestamp(SequencedTime.MaxValue, includeRejected = true)
      .map { maxTimestamp =>
        maxTimestamp.foreach { case (sequenced, effective) =>
          client.updateHead(
            sequenced,
            effective,
            effective.toApproximate,
            potentialTopologyChange = true,
          )
        }
        client
      }
}
