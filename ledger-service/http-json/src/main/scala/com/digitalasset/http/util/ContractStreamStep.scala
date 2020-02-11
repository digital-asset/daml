package com.digitalasset.http
package util

import InsertDeleteStep.{Cid, Inserts}
import Collections._

import scalaz.\/
import scalaz.std.tuple._
import scalaz.syntax.functor._

import scala.collection.generic.CanBuildFrom

private[http] sealed abstract class ContractStreamStep[+D, +C] extends Product with Serializable {
  import ContractStreamStep._

  def toInsertDelete: InsertDeleteStep[D, C] = this match {
    case Acs(inserts) => InsertDeleteStep(inserts, Map.empty)
    case LiveBegin => InsertDeleteStep(Vector.empty, Map.empty)
    case Txn(step) => step
  }

  def append[DD >: D, CC >: C: Cid](o: ContractStreamStep[DD, CC]): ContractStreamStep[DD, CC] =
    (this, o) match {
      case (Acs(l), Acs(r)) => Acs(l ++ r)
      case (Acs(_), LiveBegin) => this
      case (LiveBegin, LiveBegin) => LiveBegin // should never happen, but *shrug*
      case _ => Txn(toInsertDelete append o.toInsertDelete)
    }

  def mapPreservingIds[CC](f: C => CC): ContractStreamStep[D, CC] = this match {
    case Acs(inserts) => Acs(inserts map f)
    case LiveBegin => LiveBegin
    case Txn(step) => Txn(step mapPreservingIds f)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def partitionBimap[LD, DD, LC, CC, LDS, LCS](f: D => (LD \/ DD), g: C => (LC \/ CC))(
      implicit LDS: CanBuildFrom[Map[String, D], LD, LDS],
      LCS: CanBuildFrom[Inserts[C], LC, LCS],
  ): (LDS, LCS, ContractStreamStep[DD, CC]) =
    this match {
      case Acs(inserts) =>
        val (lcs, vcc) = inserts partitionMap g
        (LDS().result(), lcs, Acs(vcc))
      case LiveBegin => (LDS().result(), LCS().result(), LiveBegin)
      case Txn(step) => step partitionBimap (f, g) map (Txn(_))
    }

  def nonEmpty: Boolean = this match {
    case Acs(inserts) => inserts.nonEmpty
    case LiveBegin => true // unnatural wrt `toInsertDelete`, but what nonEmpty is used for here
    case Txn(step) => step.nonEmpty
  }
}

private[http] object ContractStreamStep extends WithLAV1[ContractStreamStep] {
  final case class Acs[+C](inserts: Inserts[C]) extends ContractStreamStep[Nothing, C]
  case object LiveBegin extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C]) extends ContractStreamStep[D, C]
}
