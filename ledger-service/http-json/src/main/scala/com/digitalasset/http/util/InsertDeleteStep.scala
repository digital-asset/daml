// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import com.digitalasset.http.dbbackend.Queries.DBContract
import com.digitalasset.ledger.api.v1.{event => evv1}

import scalaz.syntax.tag._

import scala.runtime.AbstractFunction1

private[http] final case class InsertDeleteStep[+D, +C](
    inserts: Vector[C],
    deletes: Map[String, D]) {
  import InsertDeleteStep._

  def append[DD >: D, CC >: C: Cid](o: InsertDeleteStep[DD, CC]): InsertDeleteStep[DD, CC] =
    InsertDeleteStep(
      appendForgettingDeletes(inserts, o),
      deletes ++ o.deletes,
    )

  def nonEmpty: Boolean = inserts.nonEmpty || deletes.nonEmpty

  def leftMap[DD](f: D => DD): InsertDeleteStep[DD, C] =
    copy(deletes = deletes transform ((_, d) => f(d)))

  /** Results undefined if cid(d) != cid(c) */
  def mapPreservingIds[CC](f: C => CC): InsertDeleteStep[D, CC] = copy(inserts = inserts map f)
}

private[http] object InsertDeleteStep {
  type LAV1 = InsertDeleteStep[evv1.ArchivedEvent, evv1.CreatedEvent]

  abstract class Cid[-C] extends (C AbstractFunction1 String)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  object Cid {
    implicit val ofDBC: Cid[DBContract[Any, Any, Any, Any]] = _.contractId
    implicit val ofAC: Cid[domain.ActiveContract[Any]] = _.contractId.unwrap
    implicit def ofFst[L](implicit L: Cid[L]): Cid[(L, Any)] = la => L(la._1)
    // ofFst and ofSnd should *not* both be defined, being incoherent together
  }

  def appendForgettingDeletes[D, C](leftInserts: Vector[C], right: InsertDeleteStep[Any, C])(
      implicit cid: Cid[C],
  ): Vector[C] =
    (if (right.deletes.isEmpty) leftInserts
     else leftInserts.filter(c => !right.deletes.isDefinedAt(cid(c)))) ++ right.inserts
}
