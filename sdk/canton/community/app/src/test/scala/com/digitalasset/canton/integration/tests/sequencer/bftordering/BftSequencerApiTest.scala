// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.bftordering

import com.digitalasset.canton.MockedNodeParameters
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{ProcessingTimeout, SessionSigningKeysConfig}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.{
  BftBlockOrdererConfig,
  BftSequencerFactory,
}
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerConfig,
  Sequencer,
  SequencerApiTest,
}
import com.digitalasset.canton.synchronizer.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.topology.SequencerId
import org.apache.pekko.stream.Materializer

class BftSequencerApiTest extends SequencerApiTest with RateLimitManagerTesting {

  override protected def supportAggregation: Boolean = true

  override protected def defaultExpectedTrafficReceipt: Option[TrafficReceipt] = None

  private def createStorage(): MemoryStorage =
    new MemoryStorage(loggerFactory, timeouts)

  private def createSynchronizerNodeParameters(): SequencerNodeParameters =
    SequencerNodeParameters(
      general = MockedNodeParameters.cantonNodeParameters(
        ProcessingTimeout()
      ),
      protocol = CantonNodeParameters.Protocol.Impl(
        sessionSigningKeys = SessionSigningKeysConfig.disabled,
        alphaVersionSupport = false,
        betaVersionSupport = true,
        dontWarnOnDeprecatedPV = false,
      ),
      maxConfirmationRequestsBurstFactor = PositiveDouble.tryCreate(1.0),
    )

  override final def createSequencer(crypto: SynchronizerCryptoClient)(implicit
      mat: Materializer
  ): Sequencer = {
    val storage = createStorage()
    val params = createSynchronizerNodeParameters()
    clock = createClock()
    driverClock = createClock()

    val factory =
      new BftSequencerFactory(
        BftBlockOrdererConfig(),
        BlockSequencerConfig(),
        health = None,
        storage,
        testedProtocolVersion,
        sequencerId,
        params,
        SequencerTestMetrics,
        loggerFactory,
        None,
      )

    factory
      .create(
        synchronizerId,
        SequencerId(synchronizerId.uid),
        clock,
        driverClock,
        crypto,
        FutureSupervisor.Noop,
        SequencerTrafficConfig(),
        runtimeReady = FutureUnlessShutdown.unit,
      )
      .futureValueUS
  }

  "BFT sequencer" when runSequencerApiTests()
}
