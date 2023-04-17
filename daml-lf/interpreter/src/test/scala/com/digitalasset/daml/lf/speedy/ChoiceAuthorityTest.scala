// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.FrontStack
import com.daml.lf.data.Ref.Party
import com.daml.lf.interpretation.Error.FailedAuthorization
import com.daml.lf.ledger.FailedAuthorization.{CreateMissingAuthorization, NoAuthorizers}
import com.daml.lf.speedy.SError.SError
import com.daml.lf.speedy.SExpr.SEApp
import com.daml.lf.speedy.SValue.{SList, SParty}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.SubmittedTransaction

import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

class ChoiceAuthorityTest extends AnyFreeSpec with Inside {

  val transactionSeed = crypto.Hash.hashPrivateKey("ChoiceAuthorityTest.scala")

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
      module M {

        record @serializable Goal = { goal: Party } ;
        template (record : Goal) = {
          precondition True;
          signatories Cons @Party [M:Goal {goal} record] (Nil @Party);
          observers Nil @Party;
          agreement "Agreement";
        };

        record @serializable T = { theSig: Party, theCon: Party, theAut: List Party, theGoal: Party };
        template (this: T) = {
          precondition True;
          signatories Cons @Party [M:T {theSig} this] Nil @Party;
          observers Nil @Party;
          agreement "Agreement";

          choice TheChoice (self) (u: Unit) : Unit,
            controllers Cons @Party [M:T {theCon} this] Nil @Party
            , authorizers M:T {theAut} this
            to
              ubind x1: ContractId M:Goal <- create @M:Goal (M:Goal { goal = M:T {theGoal} this })
              in upure @Unit ();
        };

        val call : Party -> Party -> List Party -> Party -> Update Unit =
          \(theSig: Party) ->
          \(theCon: Party) ->
          \(theAut: List Party) ->
          \(theGoal: Party) ->
          ubind
          cid : ContractId M:T <- create @M:T (M:T {theSig = theSig, theCon = theCon, theAut = theAut, theGoal = theGoal})
          in exercise @M:T TheChoice cid ();
       }
      """)

  import SpeedyTestLib.AuthRequest

  type Success = (SubmittedTransaction, List[AuthRequest])

  val alice = Party.assertFromString("Alice")
  val bob = Party.assertFromString("Bob")
  val charlie = Party.assertFromString("Charlie")

  val theSig: Party = alice
  val theCon: Party = bob

  def makeSetPartyValue(set: Set[Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def runExample(
      theAut: Set[Party],
      theGoal: Party,
  ): Either[SError, Success] = {
    import SpeedyTestLib.loggingContext
    val committers = Set(theSig, theCon)
    val example = SEApp(
      pkgs.compiler.unsafeCompile(e"M:call"),
      Array(
        SParty(theSig),
        SParty(theCon),
        makeSetPartyValue(theAut),
        SParty(theGoal),
      ),
    )
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransactionCollectAuthRequests(machine)
  }

  "Explicit choice authority" - {

    "restrict authority" in {
      inside(runExample(theAut = Set(alice), theGoal = alice)) { case Right((_, ars)) =>
        ars shouldBe List()
      }
    }

    "restrict authority, fail" in {
      inside(runExample(theAut = Set(bob), theGoal = alice)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(bob)
            cma.requiredParties shouldBe Set(alice)
          }
        }
      }
    }

    "restrict authority to empty, fail" in {
      inside(runExample(theAut = Set(), theGoal = alice)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case _: NoAuthorizers =>
          }
        }
      }
    }

    "change authority" in {
      inside(runExample(theAut = Set(charlie), theGoal = charlie)) { case Right((_, ars)) =>
        ars shouldBe List(
          AuthRequest(holding = Set(alice, bob), requesting = Set(charlie))
        )
      }
    }
  }
}
