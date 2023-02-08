// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.Party
import com.daml.lf.language.Ast.Expr
import com.daml.lf.speedy.SExpr._
import com.daml.lf.speedy.SValue._
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.SubmittedTransaction
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.Inside

import com.daml.lf.speedy.SError._

class WithAuthorityTest
    extends AnyFreeSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

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

           // nested calls to WITH_AUTHORITY...
           WITH_AUTHORITY @Unit (Cons @Party [b] Nil@Party)
           (WITH_AUTHORITY @Unit (Cons @Party [c] Nil@Party)
            (ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = c, info = 100 }
            in upure @Unit ()));
       }
      """)

  "NEW" - {
    "new test" in {
      val alice = Party.assertFromString("Alice")
      val bob = Party.assertFromString("Bob")
      val charlie = Party.assertFromString("Charlie")
      val exp: Expr = e"M:entryPoint"
      val se: SExpr = pkgs.compiler.unsafeCompile(exp)
      val example: SExpr = SEApp(se, Array(SParty(alice), SParty(bob), SParty(charlie)))
      val committers: Set[Party] = Set(alice)
      val machine: Speedy.UpdateMachine =
        Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
      val either: Either[SError, SubmittedTransaction] = SpeedyTestLib.buildTransaction(machine)
      either match {
        case Right(tx) =>
          println(s"TX=$tx") // NICK: assert the tx has expected shape, with correct auth nodes.
        case Left(e) =>
          fail(Pretty.prettyError(e).render(80))
      }

    }
  }

}
