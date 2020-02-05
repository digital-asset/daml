// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.platform.indexer.IndexerConfig._

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class IndexerConfig(
    participantId: ParticipantId,
    jdbcUrl: String,
    startupMode: IndexerStartupMode,
    restartDelay: FiniteDuration = DefaultRestartDelay,
    allowExistingSchema: Boolean = false,
)

object IndexerConfig {
  val DefaultRestartDelay: FiniteDuration = 10.seconds
}
