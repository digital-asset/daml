// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

sealed trait BenchmarkResult extends Product with Serializable

object BenchmarkResult {
  final case object Ok extends BenchmarkResult
  final case object ObjectivesViolated extends BenchmarkResult
}
