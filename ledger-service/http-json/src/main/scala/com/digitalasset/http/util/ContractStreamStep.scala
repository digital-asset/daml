// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import InsertDeleteStep.{Cid, Inserts}

import scalaz.\/
import scalaz.std.tuple._
import scalaz.syntax.functor._

import scala.collection.generic.CanBuildFrom

private[http] sealed abstract class ContractStreamStep[+D, +C] extends Product with Serializable {
  import ContractStreamStep._

  def toInsertDelete: InsertDeleteStep[D, C] = this match {
    case LiveBegin => InsertDeleteStep(Vector.empty, Map.empty)
    case Txn(step) => step
  }

  /** Forms a monoid with 0 = LiveBegin */
  def append[DD >: D, CC >: C: Cid](o: ContractStreamStep[DD, CC]): ContractStreamStep[DD, CC] =
    (this, o) match {
      case (_, LiveBegin) => this
      case (LiveBegin, _) => o
      case _ => Txn(toInsertDelete append o.toInsertDelete)
    }

  def mapPreservingIds[CC](f: C => CC): ContractStreamStep[D, CC] =
    mapStep(_ mapPreservingIds f)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def partitionBimap[LD, DD, LC, CC, LDS, LCS](f: D => (LD \/ DD), g: C => (LC \/ CC))(
      implicit LDS: CanBuildFrom[Map[String, D], LD, LDS],
      LCS: CanBuildFrom[Inserts[C], LC, LCS],
  ): (LDS, LCS, ContractStreamStep[DD, CC]) =
    this match {
      case LiveBegin => (LDS().result(), LCS().result(), LiveBegin)
      case Txn(step) => step partitionBimap (f, g) map (Txn(_))
    }

  def mapStep[DD, CC](
      f: InsertDeleteStep[D, C] => InsertDeleteStep[DD, CC]): ContractStreamStep[DD, CC] =
    this match {
      case LiveBegin => LiveBegin
      case Txn(step) => Txn(f(step))
    }

  def nonEmpty: Boolean = this match {
    case LiveBegin => true // unnatural wrt `toInsertDelete`, but what nonEmpty is used for here
    case Txn(step) => step.nonEmpty
  }
}

private[http] object ContractStreamStep extends WithLAV1[ContractStreamStep] {
  case object LiveBegin extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C]) extends ContractStreamStep[D, C]

  def acs[C](inserts: Inserts[C]): ContractStreamStep[Nothing, C] =
    Txn(InsertDeleteStep(inserts, Map.empty))
}
