// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.language.higherKinds
import scala.collection.GenTraversableOnce
import scala.collection.immutable.{Map, MapLike}

/** Halfway between the *-kinded MapLike and *->*->*-kinded MapOps. */
trait MapKOps[K, +V, +This[+TV] <: Map[K, TV] with MapKOps[K, TV, This]] extends MapLike[K, V, This[V]] { this: This[V] =>
  override def updated[V1 >: V](key: K, value: V1): This[V1] = this + ((key, value))
  override def +[V1 >: V](kv: (K, V1)): This[V1]
  override def +[V1 >: V](elem1: (K, V1), elem2: (K, V1), elems: (K, V1)*): This[V1] =
    this + elem1 + elem2 ++ elems
  override def ++[V1 >: V](xs: GenTraversableOnce[(K, V1)]): This[V1] =
    xs.seq.foldLeft(this: This[V1])(_ + _)
}
