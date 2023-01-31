// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.language.Ast.Expr
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value.{ValueInt64, ValueRecord}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.freespec.AnyFreeSpec

import com.daml.lf.speedy.SError._

class WithAuthorityTest extends AnyFreeSpec with Matchers with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  import WithAuthorityTest._

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("WithAuthorityTest.scala")

  private[this] def runUpdateExprGetTx(
      pkgs1: PureCompiledPackages
  )(e: Expr, party: Party): SubmittedTransaction = {
    val se = pkgs1.compiler.unsafeCompile(e)
    val example = SEApp(se, Array(SParty(party)))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs1, transactionSeed, example, Set(party))
    SpeedyTestLib
      .buildTransaction(machine)
      .fold(e => fail(Pretty.prettyError(e).toString()), identity)
  }

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
      module M {

        record @serializable T1 = { party: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True;
          signatories Cons @Party [M:T1 {party} record] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        val create0 : Party -> Update Unit = \(party: Party) ->
            upure @Unit ();

        val create1 : Party -> Update Unit = \(party: Party) ->
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 }
            in upure @Unit ();

        val create2 : Party -> Update Unit = \(party: Party) ->
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
              x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
            in upure @Unit ();

        val createForSomeoneElse : Party -> Party -> Update Unit =
          \(me: Party) -> \(other: Party) ->

            with_authority @Unit (Cons @Party [other] Nil@Party)
            (ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = other, info = 100 }
            in upure @Unit ());


       }
      """)

  val testCases = Table[String, List[Tree]](
    ("expression", "expected-number-of-contracts"),
    ("create0", Nil),
    ("create1", List(C(100))),
    ("create2", List(C(100), C(200))),
  )

  forEvery(testCases) { (exp: String, expected: List[Tree]) =>
    s"""$exp, contracts expected: $expected """ in {
      val party = Party.assertFromString("Alice")
      val tx: SubmittedTransaction = runUpdateExprGetTx(pkgs)(e"M:$exp", party)
      val ids: List[Tree] = shapeOfTransaction(tx)
      ids shouldBe expected
    }
  }

  "NEW" - {
    "new test" in {
      val alice = Party.assertFromString("Alice") // me
      val bob = Party.assertFromString("Bob") // other

      val exp: Expr = e"M:createForSomeoneElse"
      val se: SExpr = pkgs.compiler.unsafeCompile(exp)
      val example: SExpr = SEApp(se, Array(SParty(alice), SParty(bob)))

      val committers: Set[Party] = Set(bob) // NICK -- make it work for alice!

      val machine: Speedy.UpdateMachine =
        Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)

      val either: Either[SError, SubmittedTransaction] = SpeedyTestLib.buildTransaction(machine)

      either match { // NICK: inside
        case Left(e) =>
          fail(s"NICK-FAIL:[${Pretty.prettyError(e).render(80)}]")

        case Right(tx) =>
          // println(s"tx=\n- $tx")
          val ids: List[Tree] = shapeOfTransaction(tx)
          ids shouldBe List(C(100))
      }
    }
  }

}

object WithAuthorityTest {

  sealed trait Tree // minimal transaction tree, for purposes of writing test expectation
  final case class C(x: Long) extends Tree // Create Node
  final case class X(x: List[Tree]) extends Tree // Exercise Node
  final case class R(x: List[Tree]) extends Tree // Rollback Node
  final case class A(x: List[Tree]) extends Tree // Authority Node

  private def shapeOfTransaction(tx: SubmittedTransaction): List[Tree] = {
    def trees(nid: NodeId): List[Tree] = {
      tx.nodes(nid) match {
        case create: Node.Create =>
          create.arg match {
            case ValueRecord(_, ImmArray(_, (None, ValueInt64(n)))) =>
              List(C(n))
            case _ =>
              sys.error(s"unexpected create.arg: ${create.arg}")
          }
        case _: Node.LeafOnlyAction =>
          Nil
        case node: Node.Exercise =>
          List(X(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Rollback =>
          List(R(node.children.toList.flatMap(nid => trees(nid))))
        case node: Node.Authority =>
          List(A(node.children.toList.flatMap(nid => trees(nid))))
      }
    }
    tx.roots.toList.flatMap(nid => trees(nid))
  }

}
