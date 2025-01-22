// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.config.{CachingConfigs, DefaultProcessingTimeouts}
import com.digitalasset.canton.crypto.SynchronizerSyncCryptoClient
import com.digitalasset.canton.protocol.DynamicSynchronizerParameters
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import org.apache.pekko.stream.Materializer

// TODO(#16087) Re-enabled when Database sequencer is revived
abstract class DatabaseSequencerApiTest extends SequencerApiTest {

  def createSequencer(
      crypto: SynchronizerSyncCryptoClient
  )(implicit materializer: Materializer): CantonSequencer = {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val crypto = TestingIdentityFactory(
      TestingTopology(),
      loggerFactory,
      DynamicSynchronizerParameters.initialValues(clock, testedProtocolVersion),
    ).forOwnerAndSynchronizer(owner = mediatorId, synchronizerId)
    val metrics = SequencerMetrics.noop("database-sequencer-test")

    // we explicitly pass the parallel executor service to use as the execution context
    // otherwise the default scalatest sequential execution context will be used that causes
    // problems when we Await on the AsyncClosable for done while scheduled watermarks are
    // still being processed (that then causes a deadlock as the completed signal can never be
    // passed downstream)
    val dbConfig = TestDatabaseSequencerConfig()
    val storage = new MemoryStorage(loggerFactory, timeouts)
    val sequencerStore = SequencerStore(
      storage,
      testedProtocolVersion,
      maxBufferedEventsSize = NonNegativeInt.tryCreate(1000),
      timeouts = timeouts,
      loggerFactory = loggerFactory,
      sequencerMember = sequencerId,
      blockSequencerMode = false,
      cachingConfigs = CachingConfigs(),
    )
    DatabaseSequencer.single(
      dbConfig,
      None,
      DefaultProcessingTimeouts.testing,
      storage,
      sequencerStore,
      clock,
      synchronizerId,
      sequencerId,
      testedProtocolVersion,
      crypto,
      metrics,
      loggerFactory,
    )(executorService, tracer, materializer)
  }

  // TODO(#12405) Set to true
  override protected def supportAggregation: Boolean = false

  "DB sequencer" when runSequencerApiTests()
}
