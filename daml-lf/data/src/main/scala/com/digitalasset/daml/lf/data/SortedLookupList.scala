// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.language.higherKinds

import scalaz.{Applicative, Equal, Traverse}
import scalaz.std.tuple._
import scalaz.std.string._
import scalaz.syntax.equal._
import scalaz.syntax.traverse._

import scala.collection.immutable.HashMap

/** We use this container to pass around DAML-LF maps as flat lists in various parts of the codebase. */
// Note that keys are ordered using Utf8 ordering
final class SortedLookupList[+X] private (entries: ImmArray[(String, X)]) extends Equals {

  def mapValue[Y](f: X => Y) = new SortedLookupList(entries.map { case (k, v) => k -> f(v) })

  def toImmArray: ImmArray[(String, X)] = entries

  def keys: ImmArray[String] = entries.map(_._1)

  def values: ImmArray[X] = entries.map(_._2)

  def toHashMap: HashMap[String, X] = HashMap(entries.toSeq: _*)

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

object SortedLookupList {

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

  implicit def `SLL Equal instance`[X: Equal]: Equal[SortedLookupList[X]] =
    ScalazEqual.withNatural(Equal[X].equalIsNatural) { (self, other) =>
      self.toImmArray === other.toImmArray
    }

  implicit val `SLL covariant instance`: Traverse[SortedLookupList] =
    new Traverse[SortedLookupList] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: SortedLookupList[A])(
          f: A => G[B]): G[SortedLookupList[B]] =
        fa.toImmArray traverse (_ traverse f) map (new SortedLookupList(_))
    }

}
