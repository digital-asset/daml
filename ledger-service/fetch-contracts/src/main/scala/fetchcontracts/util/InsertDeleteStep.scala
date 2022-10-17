// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts
package util

import com.daml.http.dbbackend.Queries.DBContract
import com.daml.ledger.api.v1.{event => evv1}

import scalaz.{Monoid, \/, \/-}
import scalaz.syntax.tag._

import scala.runtime.AbstractFunction1

private[daml] final case class InsertDeleteStep[+D, +C](
    inserts: InsertDeleteStep.Inserts[C],
    deletes: Map[String, D],
) {
  import InsertDeleteStep._

  def append[DD >: D, CC >: C: Cid](o: InsertDeleteStep[DD, CC]): InsertDeleteStep[DD, CC] =
    InsertDeleteStep(
      appendForgettingDeletes(inserts, o),
      deletes ++ o.deletes,
    )

  /** NB: This is ''not'' distributive across `append`. */
  def size: Int = inserts.length + deletes.size

  def nonEmpty: Boolean = inserts.nonEmpty || deletes.nonEmpty

  def leftMap[DD](f: D => DD): InsertDeleteStep[DD, C] =
    copy(deletes = deletes transform ((_, d) => f(d)))

  /** Results undefined if cid(d) != cid(c) */
  def mapPreservingIds[CC](f: C => CC): InsertDeleteStep[D, CC] = copy(inserts = inserts map f)

  /** Results undefined if cid(d) != cid(c) */
  def partitionMapPreservingIds[LC, CC](
      f: C => (LC \/ CC)
  ): (Inserts[LC], InsertDeleteStep[D, CC]) = {
    val (_, lcs, step) = partitionBimap(\/-(_), f)(List)
    (lcs, step)
  }

  /** Results undefined if cid(cc) != cid(c) */
  def partitionBimap[LD, DD, LC, CC, LDS](f: D => (LD \/ DD), g: C => (LC \/ CC))(implicit
      LDS: collection.Factory[LD, LDS]
  ): (LDS, Inserts[LC], InsertDeleteStep[DD, CC]) = {
    import scalaz.std.tuple._, scalaz.std.either._, scalaz.syntax.traverse._
    val (lcs, ins) = inserts partitionMap (x => g(x).toEither)
    val (lds, del) = deletes.toList.partitionMap(_.traverse(x => f(x).toEither))
    (LDS.fromSpecific(lds), lcs, copy(inserts = ins, deletes = del.toMap))
  }
}

private[daml] object InsertDeleteStep extends WithLAV1[InsertDeleteStep] {
  type Inserts[+C] = Vector[C]
  val Inserts: Vector.type = Vector

  val Empty: InsertDeleteStep[Nothing, Nothing] = apply(Vector.empty, Map.empty)

  abstract class Cid[-C] extends (C AbstractFunction1 String)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  object Cid {
    implicit val ofDBC: Cid[DBContract[Any, Any, Any, Any]] = _.contractId
    implicit val ofAC: Cid[domain.ActiveContract.ResolvedCtTyId[Any]] = _.contractId.unwrap
    implicit def ofFst[L](implicit L: Cid[L]): Cid[(L, Any)] = la => L(la._1)
    // ofFst and ofSnd should *not* both be defined, being incoherent together
  }

  // we always use the Last semigroup for D
  implicit def `IDS monoid`[D, C: Cid]: Monoid[InsertDeleteStep[D, C]] =
    Monoid.instance(_ append _, Empty)

  def appendForgettingDeletes[D, C](leftInserts: Inserts[C], right: InsertDeleteStep[Any, C])(
      implicit cid: Cid[C]
  ): Inserts[C] =
    (if (right.deletes.isEmpty) leftInserts
     else leftInserts.filter(c => !right.deletes.isDefinedAt(cid(c)))) ++ right.inserts
}

private[daml] trait WithLAV1[F[_, _]] {
  type LAV1 = F[evv1.ArchivedEvent, evv1.CreatedEvent]
}
