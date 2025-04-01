// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.integration.tests.sequencer.reference.ReferenceSequencerWithTrafficControlApiTestBase
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.PostgresTest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  BftSequencerFactory,
}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.TrafficPurchasedManager
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.stream.Materializer

class BftOrderingSequencerWithTrafficControlApiTestPostgres
    extends ReferenceSequencerWithTrafficControlApiTestBase
    with PostgresTest {

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    super.cleanDb(storage).flatMap { _ =>
      import storage.api.*

      storage.update(
        DBIO.seq(
          Seq(
            "ord_epochs",
            "ord_availability_batch",
            "ord_pbft_messages_in_progress",
            "ord_pbft_messages_completed",
            "ord_metadata_output_blocks",
            "ord_p2p_endpoints",
          )
            .map(name => sqlu"truncate table #$name")*
        ),
        functionFullName,
      )
    }

  protected override final def createBlockSequencerFactory(
      clock: Clock,
      topologyFactory: TestingIdentityFactory,
      storage: DbStorage,
      params: SequencerNodeParameters,
      rateLimitManager: SequencerRateLimitManager,
      sequencerMetrics: SequencerMetrics,
  )(implicit mat: Materializer): BlockSequencerFactory =
    new BftSequencerFactory(
      BftBlockOrdererConfig(),
      BlockSequencerConfig(),
      health = None,
      storage,
      testedProtocolVersion,
      sequencerId,
      params,
      sequencerMetrics,
      loggerFactory,
      None,
    ) {
      override protected def makeRateLimitManager(
          trafficPurchasedManager: TrafficPurchasedManager,
          synchronizerSyncCryptoApi: SynchronizerCryptoClient,
          protocolVersion: ProtocolVersion,
          trafficConfig: SequencerTrafficConfig,
      ): SequencerRateLimitManager = rateLimitManager
    }
}
