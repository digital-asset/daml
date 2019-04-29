// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.collection.immutable.HashMap

/** We use this container to pass around DAML-LF maps as flat lists in various parts of the codebase. */
class SortedLookupList[+X] private (entries: ImmArray[(String, X)]) {

  def mapValue[Y](f: X => Y) = new SortedLookupList(entries.map { case (k, v) => k -> f(v) })

  def toImmArray: ImmArray[(String, X)] = entries

  def keys: ImmArray[String] = entries.map(_._1)

  def values: ImmArray[X] = entries.map(_._2)

  def toHashMap: HashMap[String, X] = HashMap(entries.toSeq: _*)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SortedLookupList[X] => other.toImmArray == entries
      case _ => false
    }
  }

  override def hashCode(): Int = entries.hashCode()

  override def toString: String =
    s"SortedLookupList(${entries.map { case (k, v) => k -> v }.toSeq.mkString(",")})"
}

object SortedLookupList {

  // Note: it's important that this ordering is the same as the DAML-LF ordering.
  private implicit val keyOrdering: Ordering[String] = UTF8.ordering

  def fromImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] = {
    entries.toSeq
      .groupBy(_._1)
      .collectFirst {
        case (k, l) if l.size > 1 => s"key $k duplicated when trying to build map"
      }
      .toLeft(new SortedLookupList(entries.toSeq.sortBy(_._1).toImmArray))
  }

  def fromSortedImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] = {
    entries
      .map(_._1)
      .toSeq
      .sliding(2)
      .collectFirst {
        case Seq(k1, k2) if keyOrdering.gteq(k1, k2) => s"the list $entries is not sorted by key"
      }
      .toLeft(new SortedLookupList(entries))
  }

  def apply[X](entries: Map[String, X]): SortedLookupList[X] =
    new SortedLookupList(ImmArray(entries.toSeq.sortBy(_._1)))

  def empty[X]: SortedLookupList[X] = new SortedLookupList(ImmArray.empty)

}
