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

  def toInsertDelete: InsertDeleteStep[D, C] = this match {
    case Acs(inserts) => InsertDeleteStep(inserts, Map.empty)
    case LiveBegin(_) => InsertDeleteStep(Vector.empty, Map.empty)
    case Txn(step, _) => step
  }

  def append[DD >: D, CC >: C: Cid](o: ContractStreamStep[DD, CC]): ContractStreamStep[DD, CC] =
    (this, o) match {
      case (Acs(inserts), Acs(oinserts)) => Acs(inserts ++ oinserts)
      case (Acs(_), LiveBegin(AbsoluteBookmark(off))) =>
        Txn(toInsertDelete, off)
      case (Acs(_) | Txn(_, _), Txn(ostep, off)) =>
        Txn(toInsertDelete append ostep, off)
      case (LiveBegin(_), Txn(_, _) | LiveBegin(_)) => o
      // the following cases should never happen in a real stream; we attempt to
      // provide definitions that make `append` totally associative, anyway
      case (Acs(_), LiveBegin(LedgerBegin)) => this
      case (LiveBegin(LedgerBegin), Acs(_)) => o
      case (LiveBegin(AbsoluteBookmark(off)), Acs(_)) => Txn(o.toInsertDelete, off)
      case (Txn(step, off), Acs(_) | LiveBegin(LedgerBegin)) =>
        Txn(step append o.toInsertDelete, off)
      case (Txn(step, _), LiveBegin(AbsoluteBookmark(off))) => Txn(step, off)
    }

  def mapPreservingIds[CC](f: C => CC): ContractStreamStep[D, CC] = this match {
    case Acs(inserts) => Acs(inserts map f)
    case lb @ LiveBegin(_) => lb
    case Txn(step, off) => Txn(step mapPreservingIds f, off)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def partitionBimap[LD, DD, LC, CC, LDS](f: D => (LD \/ DD), g: C => (LC \/ CC))(
      implicit LDS: CanBuildFrom[Map[String, D], LD, LDS],
  ): (LDS, Inserts[LC], ContractStreamStep[DD, CC]) =
    this match {
      case Acs(inserts) =>
        val (lcs, ins) = inserts partitionMap g
        (LDS().result(), lcs, Acs(ins))
      case lb @ LiveBegin(_) => (LDS().result(), Inserts.empty, lb)
      case Txn(step, off) => step partitionBimap (f, g) map (Txn(_, off))
    }

  def mapInserts[CC](f: Inserts[C] => Inserts[CC]): ContractStreamStep[D, CC] = this match {
    case Acs(inserts) => Acs(f(inserts))
    case lb @ LiveBegin(_) => lb
    case Txn(step, off) => Txn(step copy (inserts = f(step.inserts)), off)
  }

  def mapDeletes[DD](f: Map[String, D] => Map[String, DD]): ContractStreamStep[DD, C] =
    this match {
      case acs @ Acs(_) => acs
      case lb @ LiveBegin(_) => lb
      case Txn(step, off) => Txn(step copy (deletes = f(step.deletes)), off)
    }

  def nonEmpty: Boolean = this match {
    case Acs(inserts) => inserts.nonEmpty
    case LiveBegin(_) => true // unnatural wrt `toInsertDelete`, but what nonEmpty is used for here
    case Txn(step, _) => step.nonEmpty
  }
}

private[http] object ContractStreamStep extends WithLAV1[ContractStreamStep] {
  final case class Acs[+C](inserts: Inserts[C]) extends ContractStreamStep[Nothing, C]
  final case class LiveBegin(offset: BeginBookmark[domain.Offset])
      extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C], offsetAfter: domain.Offset)
      extends ContractStreamStep[D, C]
  object Txn extends WithLAV1[Txn]
}
