package com.digitalasset.http
package util

import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.syntax.apply._
import scalaz.syntax.semigroup._
import scalaz.{@@, Semigroup, Tag}

class ContractStreamStepTest
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {
  import ContractStreamStepTest._, ContractStreamStep._
  import InsertDeleteStepTest._

  behavior of "append"

  it should "be associative for valid streams" in forAll(validStreamGen) { csses =>
    whenever(csses.size >= 3) {
      forEvery(
        Table(("a", "b", "c"), csses.sliding(3).map { case Seq(a, b, c) => (a, b, c) }.toSeq: _*)) {
        case (a, b, c) =>
          (a |+| (b |+| c)) should ===((a |+| b) |+| c)
      }
    }
  }

  it should "report the last offset" in forAll(anyCssGen, anyCssGen) { (a, b) =>
    def off(css: ContractStreamStep[_, _]) = css match {
      case Acs(_) => None
      case LiveBegin(off) => off.toOption
      case Txn(_, off) => Some(off)
    }
    off(a |+| b) should ===(off(b) orElse off(a))
  }

  it should "preserve append across toInsertDelete" in forAll(anyCssGen, anyCssGen) { (a, b) =>
    (a |+| b).toInsertDelete should ===(a.toInsertDelete |+| b.toInsertDelete)
  }
}

object ContractStreamStepTest {
  import InsertDeleteStepTest._, InsertDeleteStep.Inserts, ContractStreamStep._
  import org.scalacheck.Arbitrary.arbitrary
  import org.scalacheck.Gen

  type CSS = ContractStreamStep[Unit, Cid]

  implicit val `CSS semigroup`: Semigroup[CSS] =
    Semigroup instance (_.append(_)(Cid.unwrap))

  private val offGen: Gen[domain.Offset] = Tag subst Tag.unsubst(arbitrary[String @@ Alpha])
  private val acsGen = arbitrary[Inserts[Cid]] map (Acs(_))
  private val noAcsLBGen = Gen const LiveBegin(LedgerBegin)
  private val postAcsGen = offGen map (o => LiveBegin(AbsoluteBookmark(o)))
  private val txnGen = ^(arbitrary[IDS], offGen)(Txn(_, _))

  private val validStreamGen: Gen[Seq[CSS]] = for {
    beforeAfter <- Gen.zip(
      Gen.containerOf[Vector, CSS](acsGen),
      Gen.containerOf[Vector, CSS](txnGen))
    (acsSeq, txnSeq) = beforeAfter
    liveBegin <- if (acsSeq.isEmpty) noAcsLBGen else postAcsGen
  } yield (acsSeq :+ liveBegin) ++ txnSeq

  private val anyCssGen: Gen[CSS] =
    Gen.frequency((4, acsGen), (1, noAcsLBGen), (1, postAcsGen), (4, txnGen))
}
