// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

/** Utilities for interning objects.
  * For interning strings, use String.intern.
  */
object Interning {

  /** Intern an object, that is, reuse a previously created identical object. */
  trait Internable[T] {
    val interned: java.util.Map[T, T] =
      java.util.Collections.synchronizedMap[T, T](new java.util.WeakHashMap[T, T])

    def intern(x: T): T = {
      val y = interned.putIfAbsent(x, x)
      if (y == null) x else y
    }
  }

  /** Memoize the creation of an object, that is, do not create a new object if
    * one has already been created from the same argument. */
  trait Memoizable[K, V] {
    val memoized: java.util.Map[K, V] =
      java.util.Collections.synchronizedMap[K, V](new java.util.WeakHashMap[K, V])

    def memoize(k: K, v: => V): V = {
      val v2 = memoized.computeIfAbsent(k, { _ =>
        v
      })
      if (v2 == null) v else v2
    }
  }

}
