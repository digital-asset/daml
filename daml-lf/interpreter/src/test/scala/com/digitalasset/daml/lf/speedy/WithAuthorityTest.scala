// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.FrontStack
import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
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
import org.scalatest.matchers.should.Matchers

class WithAuthorityTest extends AnyFreeSpec with Inside {

  import Matchers._ // NICK: avoid import "a"
  import WithAuthorityTest._

  "Single" - {

    "single (auth changed): A->{B}->B" in {
      inside(makeSingleCall(required = Set(b), signed = b)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Create(b))))
        shape shouldBe expected
      }
    }

    "single (auth unchanged; no auth node) A->{A}->A" in {
      inside(makeSingleCall(required = Set(aa), signed = aa)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(aa), List(Create(aa)))) // NICK: wrong!
        // val expected = List(Create(aa)) // NICK: want this!
        shape shouldBe expected
      }
    }
  }

  "Nested" - {

    "nest:A->{B}->{C}->C" in {
      inside(makeNestCall(outer = Set(b), inner = Set(c), signed = c)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(b), List(Authority(Set(c), List(Create(c))))))
        shape shouldBe expected
      }
    }
    "nest:A->{A,B}->{B,C}->C" in {
      inside(makeNestCall(outer = Set(aa, b), inner = Set(b, c), signed = c)) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(aa, b), List(Authority(Set(b, c), List(Create(c))))))
        shape shouldBe expected
      }
    }
  }
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

  val aa = Party.assertFromString("Alice") // "a" is imported from Matchers. NICK: How to prevent?
  val b = Party.assertFromString("Bob")
  val c = Party.assertFromString("Charlie")

  def makeSetPartyValue(set: Set[Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def makeSingleCall(
      required: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val requiredV = makeSetPartyValue(required)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:single"), Array(requiredV, signedV))
    val committers = Set(aa)
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  def makeNestCall(
      outer: Set[Party],
      inner: Set[Party],
      signed: Party,
  ): Either[SError, SubmittedTransaction] = {
    val outerV = makeSetPartyValue(outer)
    val innerV = makeSetPartyValue(inner)
    val signedV = SParty(signed)
    val example = SEApp(pkgs.compiler.unsafeCompile(e"M:nest"), Array(outerV, innerV, signedV))
    val committers = Set(aa)
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  // TODO #15882 -- test interaction between Authority and Exercise/Rollback nodes

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
