// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.collection.{mutable, immutable}

object Relation {

  // NOTE: this definition and specifically inversion assumes
  // that the values related to an A are non-empty
  // we treat
  //  - the empty relation mapping
  //  - and a Map that maps everything to the empty set
  // as the same
  // this fits our purposes for the moment
  /** A Relation. */
  type Relation[A, B] = immutable.Map[A, Set[B]]

  object Relation {
    def union[A, B](r1: Relation[A, B], r2: Relation[A, B]): Relation[A, B] =
      r2.foldLeft(r1) {
        case (acc, (a, bs)) =>
          acc.updated(a, acc.getOrElse(a, Set.empty[B]).union(bs))
      }

    def diff[A, B](r1: Relation[A, B], r2: Relation[A, B]): Relation[A, B] =
      r1.map { case (a, bs) => a -> r2.get(a).fold(bs)(bs diff _) }

    def invert[A, B](relation: Relation[A, B]): Relation[B, A] = {
      val result = mutable.Map[B, Set[A]]() withDefaultValue Set()
      relation.foreach {
        case (a, bs) =>
          bs.foreach(b => result(b) = result(b) + a)
      }
      result.toMap
    }

    def flatten[A, B](relation: Relation[A, B]): Iterator[(A, B)] =
      for {
        kvs <- relation.iterator
        value <- kvs._2
      } yield (kvs._1, value)
  }

}
