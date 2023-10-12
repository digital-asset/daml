// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

object SetsUtil {
  def requireDisjoint[A](xs: (Set[A], String), ys: (Set[A], String)): Unit = {
    val overlap = xs._1 intersect ys._1
    if (overlap.nonEmpty)
      throw new IllegalArgumentException(s"${xs._2} overlap with ${ys._2}. Overlap: $overlap")
  }
}
