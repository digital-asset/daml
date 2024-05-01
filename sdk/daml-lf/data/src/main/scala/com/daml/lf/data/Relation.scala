// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.data

import com.daml.lf.data

object Relation {

  // NOTE: this definition and specifically inversion assumes
  // that the values related to an A are non-empty
  // we treat
  //  - the empty relation mapping
  //  - and a Map that maps everything to the empty set
  // as the same
  // this fits our purposes for the moment
  /** A Relation. */
  @deprecated("2.4.0", "use com.daml.lf.Relation")
  type Relation[A, B] = data.Relation[A, B] // alias for Map[A, Set[B]]
  @deprecated("2.4.0", "use com.daml.lf.Relation")
  val Relation = this

  def empty[A, B]: data.Relation[A, B] = Map.empty

  def update[A, B](r: data.Relation[A, B], a: A, b: B): data.Relation[A, B] =
    r.updated(a, get(r, a) + b)

  def from[A, B](pairs: Iterable[(A, B)]): data.Relation[A, B] =
    pairs.foldLeft(empty[A, B]) { case (acc, (a, b)) => update(acc, a, b) }

  def apply[A, B](pairs: (A, B)*): data.Relation[A, B] = from(pairs)

  def union[A, B](r1: data.Relation[A, B], r2: data.Relation[A, B]): data.Relation[A, B] =
    r2.foldLeft(r1) { case (acc, (a, bs)) => acc.updated(a, get(r1, a) union bs) }

  def invert[A, B](relation: data.Relation[A, B]): data.Relation[B, A] =
    from(flatten(relation).map(_.swap))

  def flatten[A, B](relation: data.Relation[A, B]): Iterable[(A, B)] =
    relation.view.flatMap { case (a, bs) => bs.view.map(a -> _) }

  private[this] def get[A, B](r: data.Relation[A, B], a: A): Set[B] =
    r.getOrElse(a, Set.empty[B])

}
