// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.collection.immutable

object RangeUtil {

  /** Yields a sequence of ranges `(x0, x1), (x1, x2), ..., (xn-2, xn-1), (xn-1, xn)`.
    * Such that:
    * <ul>
    *   <li>`x0 = from` and `xn = to`,</li>
    *   <li>`x0 < x1 < ... < xn`, if `from < to`</li>
    *   <li>`xi-1 - xi <= maxBatchSize` for `i = 0, 1, ..., n`.</li>
    * </ul>
    * If `from >= to`, the result is `(from, to)`.
    */
  def partitionIndexRange(from: Long, to: Long, maxBatchSize: Long): immutable.Seq[(Long, Long)] = {
    if (from < to) {
      val froms = from until to by maxBatchSize
      val tos = froms.drop(1) :+ to
      froms zip tos
    } else {
      immutable.Seq(from -> to)
    }
  }

}
