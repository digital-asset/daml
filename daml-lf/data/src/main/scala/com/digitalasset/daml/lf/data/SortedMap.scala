// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.collection.immutable.HashMap

class SortedMap[+X] private (entries: List[(String, X)]) {

  def mapValue[Y](f: X => Y) = new SortedMap(entries.map { case (k, v) => k -> f(v) })

  def toList: List[(String, X)] = entries

  def keys: List[String] = entries.map(_._1)

  def values: List[X] = entries.map(_._2)

  def toHashMap: HashMap[String, X] = HashMap(entries: _*)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SortedMap[X] => other.toList == entries
      case _ => false
    }
  }

  override def hashCode(): Int = entries.hashCode()

  override def toString: String =
    s"SortedMap(${entries.map { case (k, v) => k -> v }.mkString(",")})"
}

object SortedMap {

  private implicit val keyOrdering: Ordering[String] = UTF8.ordering

  def fromList[X](entries: List[(String, X)]): Either[String, SortedMap[X]] = {
    entries
      .groupBy(_._1)
      .collectFirst {
        case (k, l) if l.size > 1 => s"key $k duplicated when trying to build map"
      }
      .toLeft(new SortedMap(entries.sortBy(_._1)))
  }

  def fromSortedList[X](entries: List[(String, X)]): Either[String, SortedMap[X]] = {
    entries
      .map(_._1)
      .sliding(2)
      .collectFirst {
        case List(k1, k2) if keyOrdering.gteq(k1, k2) => s"the list $entries is not sorted by key"
      }
      .toLeft(new SortedMap(entries))
  }

  def apply[X](entries: Map[String, X]): SortedMap[X] =
    new SortedMap(entries.toList.sortBy(_._1))

  def empty[X]: SortedMap[X] = new SortedMap(List.empty)

}
