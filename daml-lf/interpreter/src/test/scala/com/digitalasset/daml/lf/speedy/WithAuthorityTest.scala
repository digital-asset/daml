// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.FrontStack
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.interpretation.Error.FailedAuthorization
import com.daml.lf.ledger.FailedAuthorization.CreateMissingAuthorization
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr.SEApp
import com.daml.lf.speedy.SValue.{SList, SParty}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value.{ValueRecord, ValueParty}

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

class WithAuthorityTest extends AnyFreeSpec with Inside {

  val a = Party.assertFromString("Alice")
  val b = Party.assertFromString("Bob")
  val c = Party.assertFromString("Charlie")

  import WithAuthorityTest._

  "Single" - {

    "single (auth changed): A->{B}->A [FAIL]" in {
      inside(makeSingleCall(committers = Set(a), required = Set(b), signed = a)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(b)
            cma.requiredParties shouldBe Set(a)
          }
        }
      }
    }

    "single (auth changed): A->{B}->B [OK]" in {
      inside(makeSingleCall(committers = Set(a), required = Set(b), signed = b)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Create(b))))
        shape shouldBe expected
      }
    }

    "single (auth restricted/extended) {A,B}->{B,C}->A [FAIL]" in {
      inside(makeSingleCall(committers = Set(a, b), required = Set(b, c), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              cma.authorizingParties shouldBe Set(b, c)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }

    "single (auth restricted/extended) {A,B}->{B,C}->B [OK]" in {
      inside(makeSingleCall(committers = Set(a, b), required = Set(b, c), signed = b)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b, c), List(Create(b))))
          shape shouldBe expected
      }
    }

    "single (auth restricted/extended) {A,B}->{B,C}->C [OK]" in {
      inside(makeSingleCall(committers = Set(a, b), required = Set(b, c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b, c), List(Create(c))))
          shape shouldBe expected
      }
    }

    "single (auth unchanged) A->{A}->A [OK; no auth-node]" in {
      inside(makeSingleCall(committers = Set(a), required = Set(a), signed = a)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Create(a))
        shape shouldBe expected
      }
    }

    "single (auth restricted) {A,B}->{B}->B [OK; no auth-node]" in {
      inside(makeSingleCall(committers = Set(a, b), required = Set(b), signed = b)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Create(b))
          shape shouldBe expected
      }
    }

    "single (auth restricted) {A,B}->{B}->A [FAIL]" in {
      inside(makeSingleCall(committers = Set(a, b), required = Set(b), signed = a)) {
        case Left(err) =>
          inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
            inside(why) { case cma: CreateMissingAuthorization =>
              val _ = cma
              cma.authorizingParties shouldBe Set(b)
              cma.requiredParties shouldBe Set(a)
            }
          }
      }
    }
  }

  "Nested" - {

    "nest:A->{B}->{C}->C" in {
      inside(makeNestCall(committers = Set(a), outer = Set(b), inner = Set(c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(b), List(Authority(Set(c), List(Create(c))))))
          shape shouldBe expected
      }
    }
    "nest:A->{A,B}->{B,C}->C" in {
      inside(makeNestCall(committers = Set(a), outer = Set(a, b), inner = Set(b, c), signed = c)) {
        case Right(tx) =>
          val shape = shapeOfTransaction(tx)
          val expected = List(Authority(Set(a, b), List(Authority(Set(b, c), List(Create(c))))))
          shape shouldBe expected
      }
    }
  }

  // TODO #15882 -- Test that the authority gain or restriction is properly scoped
  // TODO #15882 -- test interaction between Authority and Exercise/Rollback nodes

}

object WithAuthorityTest {

  import SpeedyTestLib.loggingContext

  val transactionSeed = crypto.Hash.hashPrivateKey("WithAuthorityTest.scala")

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
      module M {

        record @serializable T1 = { signed: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True;
          signatories Cons @Party [M:T1 {signed} record] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        val single : List Party -> Party -> Update Unit =
          \(requested: List Party) -> \(signed: Party) ->
           WITH_AUTHORITY @Unit requested
           (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed, info = 100 }
            in upure @Unit ());

        val nest : List Party -> List Party -> Party -> Update Unit =
          \(outer: List Party) -> \(inner: List Party) -> \(signed: Party) ->
           WITH_AUTHORITY @Unit outer
           (WITH_AUTHORITY @Unit inner
            (ubind x1: ContractId M:T1 <- create @M:T1 M:T1 { signed = signed, info = 100 }
             in upure @Unit ()));
       }
      """)

  def makeSetPartyValue(set: Set[Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def makeSingleCall(
      committers: Set[Party],
      required: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val requiredV = makeSetPartyValue(required)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:single"), Array(requiredV, signedV))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  def makeNestCall(
      committers: Set[Party],
      outer: Set[Party],
      inner: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val outerV = makeSetPartyValue(outer)
    val innerV = makeSetPartyValue(inner)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:nest"), Array(outerV, innerV, signedV))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  sealed trait Shape // minimal transaction tree, for purposes of writing test expectation
  final case class Create(signed: Party) extends Shape
  final case class Exercise(x: List[Shape]) extends Shape
  final case class Rollback(x: List[Shape]) extends Shape
  final case class Authority(obtained: Set[Party], x: List[Shape]) extends Shape

  private def shapeOfTransaction(tx: SubmittedTransaction): List[Shape] = {
    def trees(nid: NodeId): List[Shape] = {
      tx.nodes(nid) match {
        case create: Node.Create =>
          create.arg match {
            case ValueRecord(_, ImmArray((None, ValueParty(signed)), _)) =>
              List(Create(signed))
            case _ =>
              sys.error(s"unexpected create.arg: ${create.arg}")
          }
        case _: Node.LeafOnlyAction =>
          Nil
        case node: Node.Exercise =>
          List(Exercise(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Rollback =>
          List(Rollback(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Authority =>
          List(Authority(node.obtained, node.children.toList.flatMap(nid => trees(nid))))
      }
    }
    tx.roots.toList.flatMap(nid => trees(nid))
  }
}
