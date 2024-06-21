// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.FrontStack
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.interpretation.Error.FailedAuthorization
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.ledger.FailedAuthorization.{
  CreateMissingAuthorization,
  NoAuthorizers,
}
import com.digitalasset.daml.lf.speedy.SError.SError
import com.digitalasset.daml.lf.speedy.SExpr.SEApp
import com.digitalasset.daml.lf.speedy.SValue.{SList, SParty}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.SubmittedTransaction
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers._

class ChoiceAuthorityTestV2 extends ChoiceAuthorityTest(LanguageMajorVersion.V2)

class ChoiceAuthorityTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Inside {
  import SpeedyTestLib.AuthRequest

  val transactionSeed = crypto.Hash.hashPrivateKey("ChoiceAuthorityTest.scala")

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

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

  type Success = (SubmittedTransaction, List[AuthRequest])

  val alice = Party.assertFromString("Alice")
  val bob = Party.assertFromString("Bob")
  val charlie = Party.assertFromString("Charlie")

  // In all these tests; alice is the template signatory; bob is the choice controller
  val theSig: Party = alice
  val theCon: Party = bob

  def makeSetPartyValue(set: Set[Party]): SValue = {
    SList(FrontStack(set.toList.map(SParty(_)): _*))
  }

  def runExample(
      theAut: Set[Party], // The set of parties lists in the explicit choice authority decl.
      theGoal: Party, // The signatory of the created Goal template.
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
    SpeedyTestLib
      .buildTransactionCollectRequests(machine)
      .map { case (x, ars, _) => (x, ars) } // ignoring any UpgradeVerificationRequest
  }

  "Happy" - {

    "restrict authority: {A,B}-->A (need A)" in {
      val res = runExample(theAut = Set(alice), theGoal = alice)
      inside(res) { case Right((_, ars)) =>
        ars shouldBe List()
      }
    }

    "restrict authority: {A,B}-->B (need B)" in {
      val res = runExample(theAut = Set(bob), theGoal = bob)
      inside(res) { case Right((_, ars)) =>
        ars shouldBe List()
      }
    }

    "restrict authority: {A,B}-->{A,B} (need A)" in {
      val res = runExample(theAut = Set(alice, bob), theGoal = alice)
      inside(res) { case Right((_, ars)) =>
        ars shouldBe List()
      }
    }

    "restrict authority: {A,B}-->{A,B} (need B)" in {
      val res = runExample(theAut = Set(alice, bob), theGoal = bob)
      inside(res) { case Right((_, ars)) =>
        ars shouldBe List()
      }
    }

    "change/gain authority {A,B}-->{C} (need C)" in {
      inside(runExample(theAut = Set(charlie), theGoal = charlie)) { case Right((_, ars)) =>
        ars shouldBe List(AuthRequest(holding = Set(alice, bob), requesting = Set(charlie)))
      }
    }

    "change/gain authority {A,B}-->{A,C} (need A)" in {
      inside(runExample(theAut = Set(alice, charlie), theGoal = alice)) { case Right((_, ars)) =>
        ars shouldBe List(AuthRequest(holding = Set(alice, bob), requesting = Set(charlie)))
      }
    }

    "change/gain authority {A,B}-->{B,C} (need B)" in {
      inside(runExample(theAut = Set(bob, charlie), theGoal = bob)) { case Right((_, ars)) =>
        ars shouldBe List(AuthRequest(holding = Set(alice, bob), requesting = Set(charlie)))
      }
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

    "change/gain authority {A,B}-->{C} (need A)" in {
      inside(runExample(theAut = Set(charlie), theGoal = alice)) { case Left(err) =>
        inside(err) { case SError.SErrorDamlException(FailedAuthorization(_, why)) =>
          inside(why) { case cma: CreateMissingAuthorization =>
            cma.authorizingParties shouldBe Set(charlie)
            cma.requiredParties shouldBe Set(alice)
          }
        }
      }
    }
  }
}
