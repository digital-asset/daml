// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package util

import Collections._
import InsertDeleteStep.{Cid, Inserts}

import scalaz.\/
import scalaz.std.tuple._
import scalaz.syntax.functor._

import scala.collection.generic.CanBuildFrom

private[http] sealed abstract class ContractStreamStep[+D, +C] extends Product with Serializable {
  import ContractStreamStep._

  def toInsertDelete: (Inserts[C], Map[String, D]) = this match {
    case Acs(inserts) => (inserts, Map.empty)
    case LiveBegin(_) => (Vector.empty, Map.empty)
    case Txn(step) => (step.inserts, step.deletes)
  }

  /** Forms a monoid with 0 = LiveBegin */
  def append[DD >: D, CC >: C: Cid](o: ContractStreamStep[DD, CC]): ContractStreamStep[DD, CC] =
    (this, o) match {
      case (_, LiveBegin) => this
      case (LiveBegin, _) => o
      case _ => Txn(toInsertDelete append o.toInsertDelete)
    }

  def mapPreservingIds[CC](f: C => CC): ContractStreamStep[D, CC] = this match {
    case Acs(inserts) => Acs(inserts map f)
    case lb @ LiveBegin(_) => lb
    case Txn(step) => Txn(step mapPreservingIds f)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def partitionBimap[LD, DD, LC, CC, LDS](f: D => (LD \/ DD), g: C => (LC \/ CC))(
      implicit LDS: CanBuildFrom[Map[String, D], LD, LDS],
  ): (LDS, Inserts[LC], ContractStreamStep[DD, CC]) =
    this match {
      case Acs(inserts) =>
        val (lcs, ins) = inserts partitionMap g
        (LDS.result(), lcs, Acs(ins))
      case lb @ LiveBegin(_) => (LDS().result(), Inserts.empty, lb)
      case Txn(step) => step partitionBimap (f, g) map (Txn(_))
    }

  def mapDeletes[DD](f: Map[String, D] => Map[String, DD]): ContractStreamStep[DD, C] =
    this match {
      case lb @ LiveBegin(_) => lb
      case Txn(step) => Txn(step copy (deletes = f(step.deletes)))
    }

  def nonEmpty: Boolean = this match {
    case Acs(inserts) => inserts.nonEmpty
    case LiveBegin(_) => true // unnatural wrt `toInsertDelete`, but what nonEmpty is used for here
    case Txn(step) => step.nonEmpty
  }
}

private[http] object ContractStreamStep extends WithLAV1[ContractStreamStep] {
  final case class Acs[+C](inserts: Inserts[C]) extends ContractStreamStep[Nothing, C]
  final case class LiveBegin(offset: BeginBookmark[domain.Offset])
      extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C]) extends ContractStreamStep[D, C]
}
