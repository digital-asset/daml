// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v2

import java.time.Duration

import com.daml.ledger.participant.state.{protobuf, v1}

/** Ledger configuration describing the ledger's time model.
  * Emitted in [[com.daml.ledger.participant.state.v2.Update.ConfigurationChanged]].
  *
  * @param generation           The configuration generation. Monotonically increasing.
  * @param timeModel            The time model of the ledger.
  * @param maxDeduplicationTime The WriteService promises to not reject submissions whose deduplication period
  *                             extends for less than or equal to `maxDeduplicationTime` with the reason that
  *                             the deduplication period is too long.
  */
final case class Configuration(
    generation: Long,
    timeModel: TimeModel,
    maxDeduplicationTime: Duration,
)

object Configuration {
  def decode(bytes: Array[Byte]): Either[String, Configuration] =
    v1.Configuration.decode(bytes).map(AdaptedV1ReadService.adaptLedgerConfiguration)

  def decode(config: protobuf.LedgerConfiguration): Either[String, Configuration] =
    v1.Configuration.decode(config).map(AdaptedV1ReadService.adaptLedgerConfiguration)

  def encode(config: Configuration): protobuf.LedgerConfiguration =
    v1.Configuration.encode(AdaptedV1WriteService.adaptLedgerConfiguration(config))
}
