// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

final case class TestDatabaseSequencerConfig(
    writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
    reader: SequencerReaderConfig = CommunitySequencerReaderConfig(),
    testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
) extends SequencerConfig
    with DatabaseSequencerConfig {

  override def highAvailabilityEnabled: Boolean = false
}
