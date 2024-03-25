// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.DiscardOps

import scala.annotation.nowarn
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap

/** This class provides a mutable, thread safe, version of shapeless HMap.
  *
  * Type safety is only guaranteed by the `ev` parameters that is passed to various
  * methods. Because of that, default constructors of the class are disabled via the
  * `private` keyword.
  *
  * In order to have type safety, the relation `R` must satisfy the following:
  *
  * - `R` be single-valued: if `R[A, B1]` and `R[A, B2]` are defined, then
  *   we should have `B1 = B2`.
  *
  * - If there is evidence for `R[A1, B1]` and `R[A2, B2]` for different `B1` and `B2`,
  *   then for all non-null values `x1: A1` and `x2: A2`, we must have `x1 != x2`.
  *
  * See tests for counter-examples.
  */
@SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
@nowarn("msg=parameter ev in method")
final case class ConcurrentHMap[R[_, _]] private (
    underlying: concurrent.Map[Any, Any] = new TrieMap()
) {
  def get[K, V](k: K)(implicit ev: R[K, V]): Option[V] =
    underlying.get(k).asInstanceOf[Option[V]]

  def getOrElseUpdate[K, V](k: K, v: => V)(implicit ev: R[K, V]): V =
    underlying.getOrElseUpdate(k, v).asInstanceOf[V]

  def putIfAbsent[K, V](k: K, v: V)(implicit ev: R[K, V]): Option[V] =
    underlying.putIfAbsent(k, v).asInstanceOf[Option[V]]

  def put_[K, V](k: K, v: V)(implicit ev: R[K, V]): Unit =
    underlying.put(k, v).discard

  def replace_[K, V](k: K, v: V)(implicit ev: R[K, V]): Unit =
    underlying.replace(k, v).discard

  def remove[K, V](k: K)(implicit ev: R[K, V]): Option[V] =
    underlying.remove(k).asInstanceOf[Option[V]]

  def remove_[K](k: K): Unit = underlying.remove(k).discard
}

object ConcurrentHMap {
  def empty[R[_, _]]: ConcurrentHMap[R] = new ConcurrentHMap[R]()

  def apply[R[_, _]]: ConcurrentHMapPartiallyApplied[R] =
    new ConcurrentHMapPartiallyApplied[R](false)

  private[data] class ConcurrentHMapPartiallyApplied[R[_, _]](private val dummy: Boolean)
      extends AnyVal {
    @nowarn("msg=parameter ev in method")
    def apply[K, V](values: (K, V)*)(implicit ev: R[K, V]) =
      new ConcurrentHMap[R](TrieMap.from(values))
  }
}
