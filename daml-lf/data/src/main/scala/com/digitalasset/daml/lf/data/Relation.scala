// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scala.collection.immutable.Map

object Relation {

  // NOTE: this definition and specifically inversion assumes
  // that the values related to an A are non-empty
  // we treat
  //  - the empty relation mapping
  //  - and a Map that maps everything to the empty set
  // as the same
  // this fits our purposes for the moment
  /** A Relation. */
  type Relation[A, B] = Map[A, Set[B]]

  object Relation {
    def union[A, B](r1: Relation[A, B], r2: Relation[A, B]): Relation[A, B] = {
      r2.foldLeft(r1) {
        case (acc, (a, bs)) =>
          acc.updated(a, acc.getOrElse(a, Set.empty[B]).union(bs))
      }
    }

    def invert[A, B](relation: Relation[A, B]): Relation[B, A] = {
      import scala.collection.mutable.{Map => MutMap}
      val mutMap = MutMap[B, Set[A]]() withDefaultValue Set()
      relation.foreach {
        case (a, bs) =>
          bs.foreach(b => mutMap(b) = mutMap(b) + a)
      }

      mutMap.toMap
    }
  }

}
