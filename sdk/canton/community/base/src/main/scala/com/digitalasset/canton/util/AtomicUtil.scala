// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

object AtomicUtil {

  /** Generalizes [[java.util.concurrent.atomic.AtomicReference.updateAndGet]] so that the update
    * function `f` computes both a new value and a result that is to be returned.
    *
    * For a side effect-free function `f`, this is equivalent to
    * `ref.getAndUpdate(f(_)._1).pipe(f(_)._2)`, but it avoids the second call to `f`.
    */
  def updateAndGetComputed[A <: AnyRef, B](ref: AtomicReference[A])(f: A => (A, B)): B = {
    @tailrec def go(prev: A, next: A, computed: B): B =
      if (ref.weakCompareAndSetVolatile(prev, next)) computed
      else {
        val prev2 = ref.get()
        val (next2, computed2) = if (prev eq prev2) (next, computed) else f(prev2)
        go(prev2, next2, computed2)
      }

    val prev = ref.get()
    val (next, computed) = f(prev)
    go(prev, next, computed)
  }
}
