// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import DatabaseSequencerConfig.SequencerPruningConfig

final case class TestDatabaseSequencerConfig(
    writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
    reader: SequencerReaderConfig = CommunitySequencerReaderConfig(),
    testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
    pruning: SequencerPruningConfig = SequencerPruningConfig(),
) extends SequencerConfig
    with DatabaseSequencerConfig {

  override def highAvailabilityEnabled: Boolean = false
}
