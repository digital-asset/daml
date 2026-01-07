// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.collection

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** A bounded hash map where the eldest entry gets evicted one a new entry causes the size to go
  * over the max size. A callback can be defined to act on the removal of evicted items.
  */
object BoundedMap {
  def apply[K, V](
      maxSize: Int,
      onEvict: (K, V) => Unit = (_: K, _: V) => (),
  ): mutable.Map[K, V] = new BoundedMap[K, V](maxSize, onEvict).asScala
}

private class BoundedMap[K, V](
    maxSize: Int,
    onEvict: (K, V) => Unit,
) extends util.LinkedHashMap[K, V]() {
  override def removeEldestEntry(eldest: util.Map.Entry[K, V]): Boolean =
    if (size() > maxSize) {
      onEvict(eldest.getKey, eldest.getValue)
      true
    } else {
      false
    }
}
