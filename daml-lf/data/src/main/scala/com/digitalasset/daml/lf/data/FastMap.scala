// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import scala.annotation.tailrec
import scala.reflect.ClassTag

final class FastMap[Key, Value](map: Iterable[(Key, Value)])(implicit keyClassTag: ClassTag[Key])
    extends Iterable[(Key, Value)] {

  private val capacity = map.size * 3 / 2 + 1
  private val keys = Array.ofDim[Key](capacity)
  private val values = Array.ofDim[Some[Value]](capacity)

  {
    val scores = Array.fill(capacity)(0)
    map.foreach {
      case (key, value) =>
        var currKey = key
        var currValue = Some(value)
        var i = key.hashCode() % capacity
        if (i < 0) i += capacity
        var currScore = 1
        while (keys(i) != null) {
          if (scores(i) < currScore) {
            val tmpKey = keys(i)
            val tmpValue = values(i)
            val tmpScore = scores(i)
            keys(i) = currKey
            values(i) = currValue
            scores(i) = currScore
            currKey = tmpKey
            currValue = tmpValue
            currScore = tmpScore
          }
          currScore += 1
          i = (i + 1) % capacity
        }
        keys(i) = currKey
        values(i) = currValue
        scores(i) = currScore
    }
  }

  def get(key: Key): Option[Value] = {
    @tailrec
    def loop(i: Int): Option[Value] = {
      val currKey = keys(i)
      if (currKey == null) None
      else if (currKey == key) values(i)
      else loop(i + 1 % capacity)
    }
    var i = key.hashCode() % capacity
    if (i < 0) i += capacity
    loop(i)
  }

  override def iterator: Iterator[(Key, Value)] =
    values.iterator.zipWithIndex.collect { case (Some(v), i) => keys(i) -> v }

}
