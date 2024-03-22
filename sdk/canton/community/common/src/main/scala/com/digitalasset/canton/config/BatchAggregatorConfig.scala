// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveNumeric

/** Parameters for that batcher that batches queries (e.g., to a DB).
  */
sealed trait BatchAggregatorConfig extends Product with Serializable

object BatchAggregatorConfig {
  val defaultMaximumInFlight: PositiveNumeric[Int] = PositiveNumeric.tryCreate(2)
  val defaultMaximumBatchSize: PositiveNumeric[Int] = PositiveNumeric.tryCreate(500)

  def defaultsForTesting: BatchAggregatorConfig =
    Batching(
      maximumInFlight = PositiveNumeric.tryCreate(2),
      maximumBatchSize = PositiveNumeric.tryCreate(5),
    )

  def apply(
      maximumInFlight: PositiveNumeric[Int] = BatchAggregatorConfig.defaultMaximumInFlight,
      maximumBatchSize: PositiveNumeric[Int] = BatchAggregatorConfig.defaultMaximumBatchSize,
  ): Batching =
    Batching(
      maximumInFlight = maximumInFlight,
      maximumBatchSize = maximumBatchSize,
    )

  /** @param maximumInFlight Maximum number of in-flight get queries.
    * @param maximumBatchSize Maximum number of queries in a batch.
    */
  final case class Batching(
      maximumInFlight: PositiveNumeric[Int] = BatchAggregatorConfig.defaultMaximumInFlight,
      maximumBatchSize: PositiveNumeric[Int] = BatchAggregatorConfig.defaultMaximumBatchSize,
  ) extends BatchAggregatorConfig

  final case object NoBatching extends BatchAggregatorConfig
}
