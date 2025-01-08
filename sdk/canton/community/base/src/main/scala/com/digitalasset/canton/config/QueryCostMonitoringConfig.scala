// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Configuration for monitoring the cost of db queries.
  *
  * @param every determines the duration between reports
  * @param resetOnOutput determines whether the statistics will be reset after creating a report
  * @param logOperations if true (default false), log every query operation
  * @param sortBy determines the sorting of the output (default total)
  * @param logLines determines how many lines will be logged, default 15
  */
final case class QueryCostMonitoringConfig(
    every: NonNegativeFiniteDuration,
    resetOnOutput: Boolean = true,
    logOperations: Boolean = false,
    sortBy: QueryCostSortBy = QueryCostSortBy.Total,
    logLines: PositiveInt = PositiveInt.tryCreate(15),
)

sealed trait QueryCostSortBy extends Product with Serializable
object QueryCostSortBy {
  final case object Count extends QueryCostSortBy
  final case object Mean extends QueryCostSortBy
  final case object Total extends QueryCostSortBy
  final case object StdDev extends QueryCostSortBy
}
