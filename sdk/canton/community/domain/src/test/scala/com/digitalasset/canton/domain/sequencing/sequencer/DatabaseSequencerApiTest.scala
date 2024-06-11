// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.store.NonBftDomainSequencerApiTest
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.*
import org.apache.pekko.stream.Materializer

// TODO(#18423) reenable this test once DB sequencer works with implicit member registration
abstract class DatabaseSequencerApiTest extends NonBftDomainSequencerApiTest {

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
    DatabaseSequencer.single(
      TestDatabaseSequencerConfig(),
      None,
      DefaultProcessingTimeouts.testing,
      new MemoryStorage(loggerFactory, timeouts),
      clock,
      domainId,
      sequencerId,
      testedProtocolVersion,
      crypto,
      metrics,
      loggerFactory,
      unifiedSequencer = testedUseUnifiedSequencer,
    )(executorService, tracer, materializer)
  }

  // TODO(#12405) Set to true
  override protected def supportAggregation: Boolean = false
}
