// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.interpretation.Error.FailedAuthorization
import com.digitalasset.daml.lf.ledger.FailedAuthorization._
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SExpr.SEApp
import com.digitalasset.daml.lf.speedy.SValue.{SList, SParty}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{NodeId, SubmittedTransaction}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

import scala.collection.immutable.ArraySeq

class ChoiceAuthorityTest
    extends AnyFreeSpec
    with Inside {

  val transactionSeed = crypto.Hash.hashPrivateKey("ChoiceAuthorityTest.scala")

  implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters.default

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(p"""
  metadata ( 'pkg' : '1.0.0' )

  module M {

    record @serializable Goal = { goal: Party } ;
    template (record : Goal) = {
      precondition True;
      signatories Cons @Party [M:Goal {goal} record] (Nil @Party);
      observers Nil @Party;
    };

    record @serializable T = { theSig: Party, theCon: Party, theAut: List Party, theGoal: Party };
    template (this: T) = {
      precondition True;
      signatories Cons @Party [M:T {theSig} this] Nil @Party;
      observers Nil @Party;

      choice ChoiceWithExplicitAuthority (self) (u: Unit) : Unit,
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
      in exercise @M:T ChoiceWithExplicitAuthority cid ();
   }
  """)

  val alice = Ref.Party.assertFromString("Alice")
  val bob = Ref.Party.assertFromString("Bob")
  val charlie = Ref.Party.assertFromString("Charlie")

  // In all these tests; alice is the template signatory; bob is the choice controller
  val theSig: Ref.Party = alice
  val theCon: Ref.Party = bob

  def makeSetPartyValue(set: Set[Ref.Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def runExample(
      theAut: Set[Ref.Party], // The set of parties lists in the explicit choice authority decl.
      theGoal: Ref.Party, // The signatory of the created Goal template.
  ): Either[SError, SubmittedTransaction] = {
    import SpeedyTestLib.loggingContext
    val committers = Set(theSig, theCon)
    val example = SEApp(
      pkgs.compiler.unsafeCompile(e"M:call"),
      ArraySeq(
        SParty(theSig),
        SParty(theCon),
        makeSetPartyValue(theAut),
        SParty(theGoal),
      ),
    )
    val machine = Speedy.Machine.fromUpdateSExpr(pkgs, transactionSeed, example, committers)
    SpeedyTestLib.buildTransaction(machine)
  }

  "Happy" - {

    "restrict authority: {A,B}-->A (need A)" in {
      runExample(theAut = Set(alice), theGoal = alice) shouldBe a[Right[_, _]]
    }

    "restrict authority: {A,B}-->B (need B)" in {
      runExample(theAut = Set(bob), theGoal = bob) shouldBe a[Right[_, _]]
    }

    "restrict authority: {A,B}-->{A,B} (need A)" in {
      runExample(theAut = Set(alice, bob), theGoal = alice) shouldBe a[Right[_, _]]
    }

    "restrict authority: {A,B}-->{A,B} (need B)" in {
      runExample(theAut = Set(alice, bob), theGoal = bob) shouldBe a[Right[_, _]]
    }

  }

  "Sad" - { // examples which cause Authorization failure

    "restrict authority {A,B}-->{} (empty!)" in {
      inside(runExample(theAut = Set(), theGoal = alice)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case _: NoAuthorizers =>
          }
        }
      }
    }

    "restrict authority {A,B}-->A (need B)" in {
      inside(runExample(theAut = Set(alice), theGoal = bob)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(alice)
            cma.requiredParties shouldBe Set(bob)
          }
        }
      }
    }

    "restrict authority {A,B}-->B (need A)" in {
      inside(runExample(theAut = Set(bob), theGoal = alice)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(bob)
            cma.requiredParties shouldBe Set(alice)
          }
        }
      }
    }

    "restrict authority: {A,B}-->{A,B} (need C)" in {
      inside(runExample(theAut = Set(alice, bob), theGoal = charlie)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(alice, bob)
            cma.requiredParties shouldBe Set(charlie)
          }
        }
      }
    }

    "try to gain authority {A,B} --> {C}" in {
      runExample(theAut = Set(charlie), theGoal = alice) shouldBe Left(
        SError.SErrorDamlException(
          FailedAuthorization(
            NodeId(1),
            ExerciseMissingAuthorization(
              templateId = Ref.Identifier(
                defaultParserParameters.defaultPackageId,
                Ref.QualifiedName.assertFromString("M:T"),
              ),
              choiceId = Ref.Name.assertFromString("ChoiceWithExplicitAuthority"),
              optLocation = None,
              authorizingParties = Set(alice, bob),
              requiredParties = Set(bob, charlie),
            ),
          )
        )
      )
    }
  }
}
