// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package data

import scala.collection.MapView

private[lf] class MapViewConcat[K, +V](fst: MapView[K, V], snd: MapView[K, V])
    extends MapView[K, V] {
  override def get(key: K): Option[V] =
    fst.get(key).orElse(snd.get(key))

  override def iterator: Iterator[(K, V)] =
    fst.iterator ++ snd.iterator
}
