// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerStore
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import org.apache.pekko.stream.Materializer

// TODO(#16087) Re-enabled when Database sequencer is revived
abstract class DatabaseSequencerApiTest extends SequencerApiTest {

  def createSequencer(
      crypto: DomainSyncCryptoClient
  )(implicit materializer: Materializer): CantonSequencer = {
    val clock = new SimClock(loggerFactory = loggerFactory)
    val crypto = TestingIdentityFactory(
      TestingTopology(),
      loggerFactory,
      DynamicDomainParameters.initialValues(clock, testedProtocolVersion),
    ).forOwnerAndDomain(owner = mediatorId, domainId)
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
      timeouts,
      loggerFactory,
      sequencerId,
      blockSequencerMode = false,
    )
    DatabaseSequencer.single(
      dbConfig,
      None,
      DefaultProcessingTimeouts.testing,
      storage,
      sequencerStore,
      clock,
      domainId,
      sequencerId,
      testedProtocolVersion,
      crypto,
      metrics,
      loggerFactory,
      runtimeReady = FutureUnlessShutdown.unit,
    )(executorService, tracer, materializer)
  }

  // TODO(#12405) Set to true
  override protected def supportAggregation: Boolean = false

  "DB sequencer" when runSequencerApiTests()
}
