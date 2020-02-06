// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import com.digitalasset.http.dbbackend.Queries.DBContract

import scalaz.syntax.tag._

import scala.runtime.AbstractFunction1

private[http] final case class InsertDeleteStep[+C](inserts: Vector[C], deletes: Set[String]) {
  import InsertDeleteStep._

  def append[CC >: C](o: InsertDeleteStep[CC])(implicit cid: Cid[CC]): InsertDeleteStep[CC] =
    InsertDeleteStep(
      InsertDeleteStep.appendForgettingDeletes(inserts, o)(cid),
      deletes union o.deletes,
    )

  def nonEmpty: Boolean = inserts.nonEmpty || deletes.nonEmpty

  /** Results undefined if cid(d) != cid(c) */
  def mapPreservingIds[D](f: C => D): InsertDeleteStep[D] = copy(inserts = inserts map f)
}

private[http] object InsertDeleteStep {
  abstract class Cid[-C] extends (C AbstractFunction1 String)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  object Cid {
    implicit val ofDBC: Cid[DBContract[Any, Any, Any, Any]] = _.contractId
    implicit val ofAC: Cid[domain.ActiveContract[Any]] = _.contractId.unwrap
    implicit def ofFst[L](implicit L: Cid[L]): Cid[(L, Any)] = la => L(la._1)
  }

  def appendForgettingDeletes[C](leftInserts: Vector[C], right: InsertDeleteStep[C])(
      cid: C => String,
  ): Vector[C] =
    (if (right.deletes.isEmpty) leftInserts
     else leftInserts.filter(c => !right.deletes(cid(c)))) ++ right.inserts
}
