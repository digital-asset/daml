// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.annotation.tailrec
import scala.util.Random

object SeqUtil {

  /** Picks a random subset of indices of size `size` from `xs` and returns a random permutation of the elements
    * at these indices.
    * Implements the Fisher-Yates shuffle (https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle)
    * with a separate output array.
    *
    * Note: The inside-out version of Fisher-Yates would not have to modify the input sequence,
    * but doesn't work for picking a proper subset because it would be equivalent to shuffling only the prefix of length size.
    */
  def randomSubsetShuffle[A](xs: IndexedSeq[A], size: Int, random: Random): Seq[A] = {
    val outputSize = xs.size min size max 0
    val output = Seq.newBuilder[A]
    output.sizeHint(outputSize)

    @tailrec def go(lowerBound: Int, remainingElems: IndexedSeq[A]): Unit = {
      if (lowerBound >= outputSize) ()
      else {
        val index = random.nextInt(xs.size - lowerBound)
        val chosen = remainingElems(lowerBound + index)
        output.addOne(chosen)
        val newRemainingElems =
          remainingElems.updated(lowerBound + index, remainingElems(lowerBound))
        go(lowerBound + 1, newRemainingElems)
      }
    }
    go(0, xs)
    output.result()
  }
}
