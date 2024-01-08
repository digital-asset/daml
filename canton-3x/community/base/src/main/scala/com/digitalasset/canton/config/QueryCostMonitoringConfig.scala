// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration for monitoring the cost of db queries.
  *
  * @param every determines the duration between reports
  * @param resetOnOutput determines whether the statistics will be reset after creating a report
  * @param logOperations if true (default false), log every query operation
  */
final case class QueryCostMonitoringConfig(
    every: NonNegativeFiniteDuration,
    resetOnOutput: Boolean = true,
    logOperations: Boolean = false,
)
