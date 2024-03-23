// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import ScalazEqual.{equalBy, orderBy}

import scalaz.{Applicative, Equal, Order, Traverse}
import scalaz.std.tuple._
import scalaz.std.string._
import scalaz.syntax.traverse._

import scala.collection.immutable.HashMap

/** We use this container to pass around Daml-LF text maps as flat lists in various parts of the codebase. */
// Note that keys are ordered using Utf8 ordering
final class SortedLookupList[+X] private (entries: ImmArray[(String, X)]) extends Equals {

  def isEmpty: Boolean = entries.isEmpty

  def length: Int = entries.length

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
      case other: SortedLookupList[_] if other canEqual this => other.toImmArray == entries
      case _ => false
    }
  }

  override def hashCode(): Int = entries.hashCode()

  override def toString: String =
    s"SortedLookupList(${entries.map { case (k, v) => k -> v }.toSeq.mkString(",")})"
}

object SortedLookupList extends SortedLookupListInstances {

  private[this] val EntryOrdering: Ordering[(String, _)] = { case ((key1, _), (key2, _)) =>
    Utf8.Ordering.compare(key1, key2)
  }

  private[this] def nonOrderedEntry[X](entries: ImmArray[(String, X)]): Option[(String, X)] =
    (entries.iterator zip entries.iterator.drop(1)).collectFirst {
      case (entry1, entry2) if EntryOrdering.gteq(entry1, entry2) => entry2
    }

  def fromImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] = {
    val sortedEntries = entries.toSeq.sorted(EntryOrdering).toImmArray
    nonOrderedEntry(sortedEntries) match {
      case None => Right(new SortedLookupList(sortedEntries))
      case Some((key, _)) => Left(s"key $key duplicated when trying to build map")
    }
  }

  def fromOrderedImmArray[X](entries: ImmArray[(String, X)]): Either[String, SortedLookupList[X]] =
    nonOrderedEntry(entries) match {
      case None => Right(new SortedLookupList(entries))
      case Some(_) => Left(s"the entries $entries are not sorted by key")
    }

  def apply[X](entries: Map[String, X]): SortedLookupList[X] = {
    new SortedLookupList[X](entries.to(ImmArray.ImmArraySeq).sorted(EntryOrdering).toImmArray)
  }

  def Empty: SortedLookupList[Nothing] = new SortedLookupList(ImmArray.Empty)

  implicit def `SLL Order instance`[X: Order]: Order[SortedLookupList[X]] =
    orderBy(_.toImmArray, true)

  implicit val `SLL covariant instance`: Traverse[SortedLookupList] =
    new Traverse[SortedLookupList] {
      override def traverseImpl[G[_]: Applicative, A, B](fa: SortedLookupList[A])(
          f: A => G[B]
      ): G[SortedLookupList[B]] =
        fa.toImmArray traverse (_ traverse f) map (new SortedLookupList(_))
    }

}

sealed abstract class SortedLookupListInstances {
  implicit def `SLL Equal instance`[X: Equal]: Equal[SortedLookupList[X]] =
    equalBy(_.toImmArray, true)
}
