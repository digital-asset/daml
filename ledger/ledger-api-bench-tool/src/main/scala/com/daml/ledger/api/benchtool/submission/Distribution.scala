// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.submission

/** Allows to pseudo-randomly pick an index out of a set of indices according to their weights. */
class Distribution[T](weights: List[Int], items: IndexedSeq[T]) {
  assert(weights.nonEmpty, "Weights list must not be empty.")
  assert(weights.size == items.size, "The number of weights and items must be the same.")
  assert(!weights.exists(_ < 1), "Weights must be strictly positive.")

  private val totalWeight: Long = weights.map(_.toLong).sum
  private val distribution: List[Double] =
    weights.scanLeft(0)((sum, weight) => sum + weight).map(_.toDouble / totalWeight).tail

  def choose(randomDouble: Double): T = items(index(randomDouble))

  private[submission] def index(randomDouble: Double): Int = {
    assert(randomDouble < 1.0, "Given random double must be < 1.0.")
    // Consider changing implementation to use binary search when using on large lists.
    distribution.indexWhere(_ > randomDouble)
  }

}
