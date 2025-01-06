// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data

import scala.collection.mutable

object Utils {

  def dequeueN[ElementT, NumberT](
      queue: mutable.Queue[ElementT],
      n: NumberT,
  )(implicit num: Numeric[NumberT]): Seq[ElementT] = {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var remaining = n
    queue.dequeueWhile { _ =>
      import num.*
      val left = remaining
      remaining = remaining - num.one
      left > num.zero
    }.toSeq
  }
}
