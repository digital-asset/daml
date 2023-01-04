// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts
package util

import InsertDeleteStep.{Cid, Inserts}

import scalaz.{Semigroup, \/}
import scalaz.std.tuple._
import scalaz.syntax.functor._

private[daml] sealed abstract class ContractStreamStep[+D, +C] extends Product with Serializable {
  import ContractStreamStep._

  def toInsertDelete: InsertDeleteStep[D, C] = this match {
    case Acs(inserts) => InsertDeleteStep(inserts, Map.empty)
    case LiveBegin(_) => InsertDeleteStep.Empty
    case Txn(step, _) => step
  }

  def append[DD >: D, CC >: C: Cid](o: ContractStreamStep[DD, CC]): ContractStreamStep[DD, CC] =
    (this, o) match {
      case (Acs(inserts), Acs(oinserts)) => Acs(inserts ++ oinserts)
      case (Acs(_), LiveBegin(AbsoluteBookmark(off))) =>
        Txn(toInsertDelete, off)
      case (Acs(_) | Txn(_, _), Txn(ostep, off)) =>
        Txn(toInsertDelete append ostep, off)
      case (LiveBegin(_), Txn(_, _)) => o
      // the following cases should never happen in a real stream; we attempt to
      // provide definitions that make `append` totally associative, anyway
      case (Acs(_) | LiveBegin(_), LiveBegin(LedgerBegin)) => this
      case (LiveBegin(LedgerBegin), Acs(_) | LiveBegin(_)) |
          (LiveBegin(AbsoluteBookmark(_)), LiveBegin(AbsoluteBookmark(_))) =>
        o
      case (LiveBegin(AbsoluteBookmark(off)), Acs(_)) => Txn(o.toInsertDelete, off)
      case (Txn(step, off), Acs(_) | LiveBegin(LedgerBegin)) =>
        Txn(step append o.toInsertDelete, off)
      case (Txn(step, _), LiveBegin(AbsoluteBookmark(off))) => Txn(step, off)
    }

  def mapPreservingIds[CC](f: C => CC): ContractStreamStep[D, CC] =
    mapInserts(_ map f)

  def partitionBimap[LD, DD, LC, CC, LDS](f: D => (LD \/ DD), g: C => (LC \/ CC))(implicit
      LDS: collection.Factory[LD, LDS]
  ): (LDS, Inserts[LC], ContractStreamStep[DD, CC]) =
    this match {
      case Acs(inserts) =>
        val (lcs, ins) = inserts partitionMap (x => g(x).toEither)
        (LDS.newBuilder.result(), lcs, Acs(ins))
      case lb @ LiveBegin(_) => (LDS.newBuilder.result(), Inserts.empty, lb)
      case Txn(step, off) => step.partitionBimap(f, g)(LDS).map(Txn(_, off))
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

  def bookmark: Option[BeginBookmark[domain.Offset]] = this match {
    case Acs(_) => Option.empty
    case LiveBegin(bookmark) => Some(bookmark)
    case Txn(_, offset) => Some(AbsoluteBookmark(offset))
  }
}

private[daml] object ContractStreamStep extends WithLAV1[ContractStreamStep] {
  final case class Acs[+C](inserts: Inserts[C]) extends ContractStreamStep[Nothing, C]
  final case class LiveBegin(offset: BeginBookmark[domain.Offset])
      extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C], offsetAfter: domain.Offset)
      extends ContractStreamStep[D, C]
  object Txn extends WithLAV1[Txn]

  implicit def `CSS semigroup`[D, C: Cid]: Semigroup[ContractStreamStep[D, C]] =
    Semigroup instance (_ append _)
}
