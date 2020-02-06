// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import com.digitalasset.http.dbbackend.Queries.DBContract
import scalaz.Liskov
import scalaz.Liskov.<~<

private[http] final case class InsertDeleteStep[+C](inserts: Vector[C], deletes: Set[String]) {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def append[CC >: C](
      o: InsertDeleteStep[CC],
  )(implicit cid: CC <~< DBContract[Any, Any, Any, Any]): InsertDeleteStep[CC] =
    appendWithCid(o)(
      Liskov.contra1_2[Function1, DBContract[Any, Any, Any, Any], CC, String](cid)(_.contractId),
    )

  def appendWithCid[CC >: C](o: InsertDeleteStep[CC])(cid: CC => String): InsertDeleteStep[CC] =
    InsertDeleteStep(
      InsertDeleteStep.appendForgettingDeletes(inserts, o)(cid),
      deletes union o.deletes,
    )

  def nonEmpty: Boolean = inserts.nonEmpty || deletes.nonEmpty

  /** Results undefined if cid(d) != cid(c) */
  def mapPreservingIds[D](f: C => D): InsertDeleteStep[D] = copy(inserts = inserts map f)
}

private[http] object InsertDeleteStep {
  def appendForgettingDeletes[C](leftInserts: Vector[C], right: InsertDeleteStep[C])(
      cid: C => String,
  ): Vector[C] =
    (if (right.deletes.isEmpty) leftInserts
     else leftInserts.filter(c => !right.deletes(cid(c)))) ++ right.inserts
}
