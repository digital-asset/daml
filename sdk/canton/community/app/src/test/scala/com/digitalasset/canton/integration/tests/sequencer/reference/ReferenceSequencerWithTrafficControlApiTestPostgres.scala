// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.config.StorageConfig
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.PostgresTest
import com.digitalasset.canton.synchronizer.block.SequencerDriver
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig
import com.digitalasset.canton.synchronizer.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerRateLimitManager,
  SequencerTrafficConfig,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.ReferenceSequencerDriver
import com.digitalasset.canton.synchronizer.sequencing.traffic.TrafficPurchasedManager
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.version.ProtocolVersion
import org.apache.pekko.stream.Materializer

import DriverBlockSequencerFactory.getSequencerDriverFactory

class ReferenceSequencerWithTrafficControlApiTestPostgres
    extends ReferenceSequencerWithTrafficControlApiTestBase
    with PostgresTest {

  protected override final def createBlockSequencerFactory(
      clock: Clock,
      topologyFactory: TestingIdentityFactory,
      storage: DbStorage,
      params: SequencerNodeParameters,
      rateLimitManager: SequencerRateLimitManager,
      sequencerMetrics: SequencerMetrics,
  )(implicit mat: Materializer): DriverBlockSequencerFactory[?] =
    new DriverBlockSequencerFactory[
      ReferenceSequencerDriver.Config[StorageConfig.Memory]
    ](
      getSequencerDriverFactory(DriverName, SequencerDriver.DriverApiVersion),
      ReferenceSequencerDriver.Config(StorageConfig.Memory()),
      BlockSequencerConfig(),
      None,
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
