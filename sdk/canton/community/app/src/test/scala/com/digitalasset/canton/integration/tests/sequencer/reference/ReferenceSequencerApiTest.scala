// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.sequencer.reference

import com.digitalasset.canton.MockedNodeParameters
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.config.{ProcessingTimeout, SessionSigningKeysConfig, StorageConfig}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.block.SequencerDriver
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.DriverBlockSequencerFactory
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameters
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerTrafficConfig
import com.digitalasset.canton.synchronizer.sequencer.{
  BlockSequencerConfig,
  Sequencer,
  SequencerApiTest,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.ReferenceSequencerDriver
import com.digitalasset.canton.synchronizer.sequencing.traffic.RateLimitManagerTesting
import com.digitalasset.canton.topology.SequencerId
import org.apache.pekko.stream.Materializer

class ReferenceSequencerApiTest extends SequencerApiTest with RateLimitManagerTesting {

  override final def createSequencer(crypto: SynchronizerCryptoClient)(implicit
      mat: Materializer
  ): Sequencer = {
    val storage = createStorage()
    val params = createSynchronizerNodeParameters()
    clock = createClock()
    driverClock = createClock()

    val factory =
      DriverBlockSequencerFactory.getFactory(
        DriverName,
        SequencerDriver.DriverApiVersion,
        ReferenceSequencerDriver.Config(StorageConfig.Memory()),
        BlockSequencerConfig(),
        health = None,
        storage,
        testedProtocolVersion,
        sequencerId,
        params,
        SequencerTestMetrics,
        loggerFactory,
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

  override protected def supportAggregation: Boolean = true

  override protected def defaultExpectedTrafficReceipt: Option[TrafficReceipt] = None

  protected final def createStorage(): MemoryStorage =
    new MemoryStorage(loggerFactory, timeouts)

  protected final def createSynchronizerNodeParameters(): SequencerNodeParameters =
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

  "Reference sequencer" when runSequencerApiTests()
}
