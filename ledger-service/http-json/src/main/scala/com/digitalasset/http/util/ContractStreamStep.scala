package com.digitalasset.http
package util

import InsertDeleteStep.{Cid, Inserts}

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
}

private[http] object ContractStreamStep {
  final case class Acs[+C](inserts: Inserts[C]) extends ContractStreamStep[Nothing, C]
  case object LiveBegin extends ContractStreamStep[Nothing, Nothing]
  final case class Txn[+D, +C](step: InsertDeleteStep[D, C]) extends ContractStreamStep[D, C]
}
