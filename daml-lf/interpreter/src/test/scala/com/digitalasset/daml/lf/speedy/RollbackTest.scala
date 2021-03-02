// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.PureCompiledPackages
import com.daml.lf.data.Ref.Party
import com.daml.lf.language.Ast._
import com.daml.lf.speedy.Compiler.FullStackTrace
import com.daml.lf.speedy.SResult.SResultScenarioCommit
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.Node
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.validation.Validation
import com.daml.lf.value.Value

import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class ExceptionTest extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  private def typeAndCompile(pkg: Package): PureCompiledPackages = {
    val rawPkgs = Map(defaultParserParameters.defaultPackageId -> pkg)
    Validation.checkPackage(rawPkgs, defaultParserParameters.defaultPackageId, pkg)
    data.assertRight(
      PureCompiledPackages(rawPkgs, Compiler.Config.Default.copy(stacktracing = FullStackTrace))
    )
  }

  private def contractIdsInOrder_complete(tx: SubmittedTransaction): Seq[Value.ContractId] = {
    tx.fold(Vector.empty[Value.ContractId]) {
      case (acc, (_, create: Node.NodeCreate[Value.ContractId])) => acc :+ create.coid
      case (acc, _) => acc
    }
  }

  private def runUpdateExpr_GetTx(pkgs1: PureCompiledPackages)(e: Expr): SubmittedTransaction = {
    def transactionSeed: crypto.Hash = crypto.Hash.hashPrivateKey("RollbackTest.scala")
    val m = Speedy.Machine.fromScenarioExpr(pkgs1, transactionSeed, e)
    val res = m.run()
    res match {
      case SResultScenarioCommit(_, tx, _, _) =>
        tx
      case _ =>
        sys.error(s"unexpected res: $res")
    }
  }

  "Correct number of contracts are created" should {

    val pkgs: PureCompiledPackages = typeAndCompile(p"""
      module M {

        record @serializable MyException = { message: Text } ;

        exception MyException = {
          message \(e: M:MyException) -> M:MyException {message} e
        };

        record @serializable T1 = { party: Party, info: Int64 } ;
        template (record : T1) = {
          precondition True,
          signatories Cons @Party [(M:T1 {party} record)] (Nil @Party),
          observers Nil @Party,
          agreement "Agreement",
          choices {
          }
        };

        val create0 : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
            upure @Unit ());

        val create1 : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 }
            in upure @Unit ());

        val create2 : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
              x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
            in upure @Unit ());

        val create3 : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
              x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 };
              x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
            in upure @Unit ());

        val create3nested : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
            ubind
              u1: Unit <-
                ubind
                  x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                  x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
                in upure @Unit ();
              x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
            in upure @Unit ());

        val create3catchNoThrow : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
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
            in upure @Unit ());

        val create3throwAndCatch : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
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
            in upure @Unit ());

        val create3throwAndOuterCatch : Party -> Scenario Unit = \(party: Party) ->
          commit @Unit party (
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
            in upure @Unit ());

       }
      """)

    val party = Party.assertFromString("Alice")
    val lit: PrimLit = PLParty(party)
    val arg: Expr = EPrimLit(lit)

    "create 0 contracts" in {
      val example: Expr = EApp(e"M:create0", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 0
    }

    "create 1 contract" in {
      val example: Expr = EApp(e"M:create1", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 1
    }

    "create 2 contracts" in {
      val example: Expr = EApp(e"M:create2", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 2
    }

    "create 3 contracts" in {
      val example: Expr = EApp(e"M:create3", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 3
    }
    "create 3 contracts (nested)" in {
      val example: Expr = EApp(e"M:create3nested", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 3
    }
    "create 3 contracts (catch, no throw)" in {
      val example: Expr = EApp(e"M:create3catchNoThrow", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 3
    }
    "create 1 contracts (throw & catch)" in {
      val example: Expr = EApp(e"M:create3throwAndCatch", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 1
    }
    "create 1 contracts (throw & outer catch)" in {
      val example: Expr = EApp(e"M:create3throwAndOuterCatch", arg)
      val tx: SubmittedTransaction = runUpdateExpr_GetTx(pkgs)(example)
      val ids: Seq[Value.ContractId] = contractIdsInOrder_complete(tx)
      ids.size shouldBe 1
    }

  }

}
