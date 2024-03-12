// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.configuration

import java.time.Duration

/** Ledger configuration describing the ledger's time model.
  *
  * @param generation                The configuration generation. Monotonically increasing.
  * @param timeModel                 The time model of the ledger. Specifying the time-to-live bounds for Ledger API commands.
  * @param maxDeduplicationDuration  The maximum time window during which commands can be deduplicated.
  */
final case class Configuration(
    generation: Long,
    timeModel: LedgerTimeModel,
    maxDeduplicationDuration: Duration,
)

object Configuration {

  /** The first configuration generation, by convention. */
  private val StartingGeneration = 1L

  /** A duration of 1 day is likely to be longer than any application will keep retrying, barring
    * very strange events, and should work for most ledger and participant configurations.
    */
  private val reasonableMaxDeduplicationDuration: Duration = Duration.ofDays(1)

  val reasonableInitialConfiguration: Configuration = Configuration(
    generation = StartingGeneration,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationDuration = reasonableMaxDeduplicationDuration,
  )

}
