// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.memory

import com.digitalasset.canton.SequencerCounterDiscriminator
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.store.SequencerCounterTrackerStore

class InMemorySequencerCounterTrackerStore(
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends SequencerCounterTrackerStore
    with NamedLogging {
  override protected[store] val cursorStore =
    new InMemoryCursorPreheadStore[SequencerCounterDiscriminator](loggerFactory)

  override def onClosed(): Unit = ()
}
