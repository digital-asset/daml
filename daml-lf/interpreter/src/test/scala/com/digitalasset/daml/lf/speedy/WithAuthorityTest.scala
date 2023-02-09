// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.speedy.SExpr.{SExpr, SEApp}
import com.daml.lf.speedy.SValue.SParty
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value.{ValueInt64, ValueRecord}

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class WithAuthorityTest extends AnyFreeSpec with Matchers with Inside {

  import WithAuthorityTest._
  import SpeedyTestLib.loggingContext

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("WithAuthorityTest.scala")

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
      module M {

        record @serializable T1 = { party: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True;
          signatories Cons @Party [M:T1 {party} record] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        val entryPoint : Party-> Party -> Party -> Update Unit =
          \(a: Party) -> \(b: Party) -> \(c: Party) ->
           WITH_AUTHORITY @Unit (Cons @Party [a,b] Nil@Party)
           (WITH_AUTHORITY @Unit (Cons @Party [b,c] Nil@Party)
            (ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = c, info = 100 }
            in upure @Unit ()));
       }
      """)

  "WithAuthorityTest" - {
    val a = Party.assertFromString("Alice")
    val b = Party.assertFromString("Bob")
    val c = Party.assertFromString("Charlie")

    val committers = Set(a)

    "{A}->{A,B}->{B,C}->{C}" in {
      val se: SExpr = pkgs.compiler.unsafeCompile(e"M:entryPoint")
      val example: SExpr = SEApp(se, Array(SParty(a), SParty(b), SParty(c)))
      val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
      val either = SpeedyTestLib.buildTransaction(machine)
      inside(either) { case Right(tx) =>
        val shape = shapeOfTransaction(tx)
        val expected = List(Authority(Set(a, b), List(Authority(Set(b, c), List(Create(100))))))
        shape shouldBe expected
      }
    }

  }
}

object WithAuthorityTest {

  // TODO #15882 -- test interaction between Authority and Exercise/Rollback nodes

  sealed trait Shape // minimal transaction tree, for purposes of writing test expectation
  final case class Create(x: Long) extends Shape
  final case class Exercise(x: List[Shape]) extends Shape
  final case class Rollback(x: List[Shape]) extends Shape
  final case class Authority(obtained: Set[Party], x: List[Shape]) extends Shape

  private def shapeOfTransaction(tx: SubmittedTransaction): List[Shape] = {
    def trees(nid: NodeId): List[Shape] = {
      tx.nodes(nid) match {
        case create: Node.Create =>
          create.arg match {
            case ValueRecord(_, ImmArray(_, (None, ValueInt64(n)))) =>
              List(Create(n))
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
