// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

/** Allows to pseudo-randomly pick an index out of a set of indices according to their weights. */
class Distribution(weights: List[Int]) {
  assert(weights.nonEmpty, "Weights list must not be empty.")
  assert(!weights.exists(_ < 1), "Weights must be strictly positive.")

  def index(randomDouble: Double): Int = {
    assert(randomDouble < 1.0, "Given random double must be < 1.0.")
    // Consider changing implementation to use binary search when using on large lists.
    distribution.indexWhere(_ > randomDouble)
  }

  private lazy val totalWeight: Long = weights.map(_.toLong).sum
  private lazy val distribution: List[Double] =
    weights.scanLeft(0)((sum, weight) => sum + weight).map(_.toDouble / totalWeight).tail

}
