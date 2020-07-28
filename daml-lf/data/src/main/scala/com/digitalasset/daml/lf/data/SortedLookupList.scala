// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, orderBy}

import scala.language.higherKinds
import scalaz.{Applicative, Equal, Order, Traverse}
import scalaz.std.tuple._
import scalaz.std.string._
import scalaz.syntax.traverse._

import scala.collection.immutable.HashMap

/** We use this container to pass around DAML-LF text maps as flat lists in various parts of the codebase. */
// Note that keys are ordered using Utf8 ordering
final class SortedLookupList[+X] private (entries: ImmArray[(String, X)]) extends Equals {

  def mapValue[Y](f: X => Y) = new SortedLookupList(entries.map { case (k, v) => k -> f(v) })

  def toImmArray: ImmArray[(String, X)] = entries

  def keys: ImmArray[String] = entries.map(_._1)

  def values: ImmArray[X] = entries.map(_._2)

  def iterator: Iterator[(String, X)] = entries.iterator

  def toHashMap: HashMap[String, X] = HashMap(entries.toSeq: _*)

  def foreach(f: ((String, X)) => Unit): Unit = entries.foreach(f)

  override def canEqual(that: Any): Boolean = that.isInstanceOf[SortedLookupList[_]]

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SortedLookupList[X] if other canEqual this => other.toImmArray == entries
      case _ => false
    }
  }

  override def hashCode(): Int = entries.hashCode()

  override def toString: String =
    s"SortedLookupList(${entries.map { case (k, v) => k -> v }.toSeq.mkString(",")})"
}

object SortedLookupList extends SortedLookupListInstances {

  def fromImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] = {
    entries.toSeq
      .groupBy(_._1)
      .collectFirst {
        case (k, l) if l.size > 1 => s"key $k duplicated when trying to build map"
      }
      .toLeft(new SortedLookupList(entries.toSeq.sortBy(_._1)(Utf8.Ordering).toImmArray))
  }

  def fromSortedImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] = {
    entries
      .map(_._1)
      .toSeq
      .sliding(2)
      .collectFirst {
        case Seq(k1, k2) if Utf8.Ordering.gteq(k1, k2) => s"the list $entries is not sorted by key"
      }
      .toLeft(new SortedLookupList(entries))
  }

  def apply[X](entries: Map[String, X]): SortedLookupList[X] =
    new SortedLookupList(ImmArray(entries.toSeq.sortBy(_._1)))

  def empty[X]: SortedLookupList[X] = new SortedLookupList(ImmArray.empty)

  implicit def `SLL Order instance`[X: Order]: Order[SortedLookupList[X]] =
    orderBy(_.toImmArray, true)

  implicit val `SLL covariant instance`: Traverse[SortedLookupList] =
    new Traverse[SortedLookupList] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: SortedLookupList[A])(
          f: A => G[B]): G[SortedLookupList[B]] =
        fa.toImmArray traverse (_ traverse f) map (new SortedLookupList(_))
    }

}

sealed abstract class SortedLookupListInstances {
  implicit def `SLL Equal instance`[X: Equal]: Equal[SortedLookupList[X]] =
    equalBy(_.toImmArray, true)
}
