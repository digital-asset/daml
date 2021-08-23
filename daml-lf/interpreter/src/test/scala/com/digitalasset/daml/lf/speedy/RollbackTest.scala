// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party
import com.daml.lf.language.Ast.{Package, Expr, PrimLit, PLParty, EPrimLit, EApp}
import com.daml.lf.language.{LanguageVersion, Interface}
import com.daml.lf.speedy.Compiler.FullStackTrace
import com.daml.lf.speedy.PartialTransaction.{CompleteTransaction, IncompleteTransaction, LeafNode}
import com.daml.lf.speedy.SResult.SResultFinalValue
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.Node.{NodeCreate, NodeExercises, NodeRollback}
import com.daml.lf.transaction.NodeId
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.validation.Validation
import com.daml.lf.value.Value.{ValueRecord, ValueInt64}

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class ExceptionTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import ExceptionTest._

  implicit val defaultParserParameters: ParserParameters[this.type] = {
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("pkgId"),
      languageVersion = LanguageVersion.v1_dev,
    )
  }

  private def typeAndCompile(pkg: Package): PureCompiledPackages = {
    import defaultParserParameters.defaultPackageId
    val rawPkgs = Map(defaultPackageId -> pkg)
    Validation.checkPackage(Interface(rawPkgs), defaultPackageId, pkg)
    val compilerConfig = Compiler.Config.Dev.copy(stacktracing = FullStackTrace)
    PureCompiledPackages.assertBuild(rawPkgs, compilerConfig)
  }

  private def runUpdateExprGetTx(
      pkgs1: PureCompiledPackages
  )(e: Expr, party: Party): SubmittedTransaction = {
    def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("RollbackTest.scala")
    val machine = Speedy.Machine.fromUpdateExpr(pkgs1, transactionSeed, e, party)
    val res = machine.run()
    res match {
      case _: SResultFinalValue =>
        machine.withOnLedger("RollbackTest") { onLedger =>
          onLedger.ptx.finish match {
            case IncompleteTransaction(_) =>
              sys.error("unexpected IncompleteTransaction")
            case CompleteTransaction(tx, _, _) =>
              tx
          }
        }
      case _ =>
        sys.error(s"unexpected res: $res")
    }
  }

  val pkgs: PureCompiledPackages = typeAndCompile(p"""
      module M {

        record @serializable MyException = { message: Text } ;
        exception MyException = {
          message \(e: M:MyException) -> M:MyException {message} e
        };

        record @serializable T1 = { party: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True,
          signatories Cons @Party [M:T1 {party} record] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {
            choice Ch1 (self) (i : Unit) : Unit
            , controllers Cons @Party [M:T1 {party} record] (Nil @Party)
            to
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
              in upure @Unit (),

            choice Ch2 (self) (i : Unit) : Unit
            , controllers Cons @Party [M:T1 {party} record] (Nil @Party)
            to
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
                u: Unit <- throw @(Update Unit) @M:MyException (M:MyException {message = "oops"});
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
              in upure @Unit (),

            choice ChControllerThrow (self) (i : Unit) : Unit
            , controllers (throw @(List Party) @M:MyException (M:MyException {message = "oops"}))
            to
              upure @Unit ()
          }
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

        val exer3 : Party -> Update Unit = \(party: Party) ->
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };

              u: Unit <-
                try @Unit
                  ubind
                    u: Unit <- exercise @M:T1 ChControllerThrow x1 ()
                  in upure @Unit ()
                catch e -> Some @(Update Unit) (upure @Unit ())

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
    ("exer3", List[Tree](C(100))),
  )

  forEvery(testCases) { (exp: String, expected: List[Tree]) =>
    s"""$exp, contracts expected: $expected """ in {
      val party = Party.assertFromString("Alice")
      val lit: PrimLit = PLParty(party)
      val arg: Expr = EPrimLit(lit)
      val example: Expr = EApp(e"M:$exp", arg)
      val tx: SubmittedTransaction = runUpdateExprGetTx(pkgs)(example, party)
      val ids: List[Tree] = shapeOfTransaction(tx)
      ids shouldBe expected
    }
  }

}

object ExceptionTest {

  sealed trait Tree //minimal transaction tree, for purposes of writing test expectation
  final case class C(x: Long) extends Tree //Create Node
  final case class X(x: List[Tree]) extends Tree //Exercise Node
  final case class R(x: List[Tree]) extends Tree //Rollback Node

  private def shapeOfTransaction(tx: SubmittedTransaction): List[Tree] = {
    def trees(nid: NodeId): List[Tree] = {
      tx.nodes(nid) match {
        case create: NodeCreate[_] =>
          create.arg match {
            case ValueRecord(_, ImmArray(_, (None, ValueInt64(n)))) =>
              List(C(n))
            case _ =>
              sys.error(s"unexpected create.arg: ${create.arg}")
          }
        case _: LeafNode =>
          Nil
        case node: NodeExercises[_, _] =>
          List(X(node.children.toList.flatMap(nid => trees(nid))))
        case node: NodeRollback[_] =>
          List(R(node.children.toList.flatMap(nid => trees(nid))))
      }
    }
    tx.roots.toList.flatMap(nid => trees(nid))
  }

}
