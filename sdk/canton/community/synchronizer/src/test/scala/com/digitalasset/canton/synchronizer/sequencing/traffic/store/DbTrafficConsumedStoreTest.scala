// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.db.DbTest
import com.digitalasset.canton.synchronizer.sequencer.SequencerWriterConfig
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerStore
import com.digitalasset.canton.synchronizer.sequencing.traffic.store.db.DbTrafficConsumedStore
import com.digitalasset.canton.topology.{DefaultTestIdentities, Member}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

trait DbTrafficConsumedStoreTest extends AsyncWordSpec with BaseTest with TrafficConsumedStoreTest {
  this: DbTest =>

  private lazy val sequencerStore = SequencerStore(
    storage,
    testedProtocolVersion,
    bufferedEventsMaxMemory = SequencerWriterConfig.DefaultBufferedEventsMaxMemory,
    bufferedEventsPreloadBatchSize = SequencerWriterConfig.DefaultBufferedEventsPreloadBatchSize,
    timeouts,
    loggerFactory,
    blockSequencerMode = true,
    sequencerMember = DefaultTestIdentities.sequencerId,
    cachingConfigs = CachingConfigs(),
  )
  def registerMemberInSequencerStore(member: Member): FutureUnlessShutdown[Unit] =
    sequencerStore.registerMember(member, CantonTimestamp.Epoch).map(_ => ())

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(sqlu"truncate table seq_traffic_control_consumed_journal"),
      functionFullName,
    )
  }

  "TrafficConsumedStore" should {
    behave like trafficConsumedStore(() =>
      new DbTrafficConsumedStore(
        storage,
        timeouts,
        loggerFactory,
      )
    )
  }
}
