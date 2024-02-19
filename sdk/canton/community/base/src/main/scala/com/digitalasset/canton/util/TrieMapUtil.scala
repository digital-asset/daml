// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import scala.collection.concurrent.TrieMap

object TrieMapUtil {

  /** Idempotent insertion into TrieMap.
    *
    * - Inserts the key-value pair if the key is absent.
    * - Idempotency: Ignores the insertion if the key already exists and the existing value is equal to the new value.
    * - Fails if the key already exists and the existing value differs to the new value.
    *
    * @param errorFn: Takes the key, oldValue, newValue and returns an appropriate error.
    */
  def insertIfAbsent[K, V, E](
      map: TrieMap[K, V],
      key: K,
      newValue: V,
      errorFn: (K, V, V) => E,
  ): Either[E, Unit] = {

    map.putIfAbsent(key, newValue) match {
      case None => Right(())
      case Some(oldValue) =>
        Either.cond(oldValue == newValue, (), errorFn(key, oldValue, newValue))
    }
  }

  def insertIfAbsent[K, V, E](
      map: TrieMap[K, V],
      key: K,
      newValue: V,
      staticErrorFn: => E,
  ): Either[E, Unit] =
    insertIfAbsent(map, key, newValue, (_: K, _: V, _: V) => staticErrorFn)

}
