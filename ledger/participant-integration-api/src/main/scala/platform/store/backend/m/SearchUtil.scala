// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.m

import scala.annotation.tailrec
import scala.collection.Searching.{Found, InsertionPoint, SearchResult}
import scala.math.Ordering

object SearchUtil {

  def rightMostBinarySearch[CT, ST](
      elem: ST,
      c: IndexedSeq[CT],
      searchExtractor: CT => ST,
  )(implicit ord: Ordering[ST]): SearchResult = {
    val length = c.size

    def rightMostOf(i: Int): Int =
      Iterator
        .iterate(i)(_ + 1)
        .takeWhile(_ < length)
        .takeWhile(i => searchExtractor(c(i)) == elem)
        .reduce((_, x) => x)

    @tailrec
    def binarySearch(
        elem: ST,
        from: Int,
        to: Int,
    ): SearchResult = {
      if (from < 0) binarySearch(elem, 0, to)
      else if (to > length) binarySearch(elem, from, length)
      else if (to <= from) InsertionPoint(from)
      else {
        val idx = from + (to - from - 1) / 2
        math.signum(ord.compare(elem, searchExtractor(c(idx)))) match {
          case -1 => binarySearch(elem, from, idx)
          case 1 => binarySearch(elem, idx + 1, to)
          case _ => Found(rightMostOf(idx))
        }
      }
    }

    binarySearch(elem, 0, length)
  }

  def rangeIterator[CT, ST](
      startExclusive: Option[ST],
      endInclusive: ST,
      c: IndexedSeq[CT],
  )(searchExtractor: CT => ST)(implicit ord: Ordering[ST]): Iterator[CT] = {
    val length = c.size
    val fromInclusive =
      startExclusive match {
        case Some(startExclusive) =>
          rightMostBinarySearch(startExclusive, c, searchExtractor) match {
            case Found(i) => i + 1
            case InsertionPoint(i) => i
          }
        case None => 0
      }
    val toInclusive =
      rightMostBinarySearch(endInclusive, c, searchExtractor) match {
        case Found(i) => i
        case InsertionPoint(i) => i - 1
      }
    Iterator
      .iterate(fromInclusive)(_ + 1)
      .dropWhile(_ < 0)
      .takeWhile(_ < length)
      .takeWhile(_ <= toInclusive)
      .map(c)
  }

}
