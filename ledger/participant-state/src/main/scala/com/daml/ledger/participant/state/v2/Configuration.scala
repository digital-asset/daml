// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Duration

import com.daml.ledger.participant.state.v1.TimeModel

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v1.Update.ConfigurationChanged]].
  *
  * @param generation The configuration generation. Monotonically increasing.
  * @param timeModel The time model of the ledger.
  * @param deduplicationPeriodLengthGuarantee A lower bound on the length of the deduplication period for command submissions
  *                                           that the WriteService provides.
  */
final case class Configuration(
    generation: Long,
    timeModel: TimeModel,
    deduplicationPeriodLengthGuarantee: Duration,
)

// Serialization and deserialization omitted
