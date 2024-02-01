// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.Party
import com.daml.lf.language.Ast.Expr
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value.{ValueInt64, ValueRecord}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class RollbackTestV2 extends RollbackTest(LanguageMajorVersion.V2)

class RollbackTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  import RollbackTest._

  private[this] implicit val defaultParserParameters: ParserParameters[RollbackTest.this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)

  private[this] val transactionSeed = crypto.Hash.hashPrivateKey("RollbackTest.scala")

  private[this] def runUpdateExprGetTx(
      pkgs1: PureCompiledPackages
  )(e: Expr, party: Party): SubmittedTransaction = {
    val se = pkgs1.compiler.unsafeCompile(e)
    val example = SEApp(se, Array(SParty(party)))
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs1, transactionSeed, example, Set(party))
    SpeedyTestLib
      .buildTransaction(machine)
      .fold(e => fail(Pretty.prettyError(e).render(80)), identity)
  }

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
  metadata ( 'pkg' : '1.0.0' )

  module M {

    record @serializable MyException = { message: Text } ;
    exception MyException = {
      message \(e: M:MyException) -> M:MyException {message} e
    };

    record @serializable T1 = { party: Party, info: Int64 } ;
    template (record : T1) = {
      precondition True;
      signatories Cons @Party [M:T1 {party} record] (Nil @Party);
      observers Nil @Party;
      choice Ch1 (self) (i : Unit) : Unit,
        controllers Cons @Party [M:T1 {party} record] (Nil @Party)
        to
          ubind
            x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
            x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
          in upure @Unit ();
      choice Ch2 (self) (i : Unit) : Unit,
        controllers Cons @Party [M:T1 {party} record] (Nil @Party)
        to
          ubind
            x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
            u: Unit <- throw @(Update Unit) @M:MyException (M:MyException {message = "oops"});
            x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
          in upure @Unit ();
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

    val create3 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
          x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 };
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3nested : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
              x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
            in upure @Unit ();
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3catchNoThrow : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3throwAndCatch : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in throw @(Update Unit) @M:MyException (M:MyException {message = "oops"})
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3throwAndOuterCatch : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              try @Unit
                ubind
                  x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                  x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
                in throw @(Update Unit) @M:MyException (M:MyException {message = "oops"})
              catch e -> None @(Update Unit)
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();


    val exer1 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };

          u: Unit <-
            try @Unit
              ubind
                u: Unit <- exercise @M:T1 Ch1 x1 ();
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ());

          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();


    val exer2 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };

          u: Unit <-
            try @Unit
              ubind
                u: Unit <- exercise @M:T1 Ch2 x1 ();
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ());

          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

   }
  """)

  val testCases = Table[String, List[Tree]](
    ("expression", "expected-number-of-contracts"),
    ("create0", Nil),
    ("create1", List(C(100))),
    ("create2", List(C(100), C(200))),
    ("create3", List(C(100), C(200), C(300))),
    ("create3nested", List(C(100), C(200), C(300))),
    ("create3catchNoThrow", List(C(100), C(200), C(300))),
    ("create3throwAndCatch", List[Tree](R(List(C(100), C(200))), C(300))),
    ("create3throwAndOuterCatch", List[Tree](R(List(C(100), C(200))), C(300))),
    ("exer1", List[Tree](C(100), X(List(C(400), C(500))), C(200), C(300))),
    ("exer2", List[Tree](C(100), R(List(X(List(C(400))))), C(300))),
  )

  forEvery(testCases) { (exp: String, expected: List[Tree]) =>
    s"""$exp, contracts expected: $expected """ in {
      val party = Party.assertFromString("Alice")
      val tx: SubmittedTransaction = runUpdateExprGetTx(pkgs)(e"M:$exp", party)
      val ids: List[Tree] = shapeOfTransaction(tx)
      ids shouldBe expected
    }
  }
}

object RollbackTest {

  sealed trait Tree // minimal transaction tree, for purposes of writing test expectation
  final case class C(x: Long) extends Tree // Create Node
  final case class X(x: List[Tree]) extends Tree // Exercise Node
  final case class R(x: List[Tree]) extends Tree // Rollback Node

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
      }
    }
    tx.roots.toList.flatMap(nid => trees(nid))
  }

}
