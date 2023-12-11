// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.{Ast, LanguageMajorVersion}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.{SubmittedTransaction, TransactionVersion, Versioned}
import com.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class LimitsSpecV1 extends LimitsSpec(LanguageMajorVersion.V1)
class LimitsSpecV2 extends LimitsSpec(LanguageMajorVersion.V2)

class LimitsSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with TableDrivenPropertyChecks {

  import SpeedyTestLib.loggingContext

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  val pkg = p""" metadata ( '-limits-spec-' : '1.0.0' )
  module Mod {

    record @serializable T = {
      signatories: List Party,
      observers: List Party
    };

    record @serializable NoOpArg = { controllers: List Party, observers: List Party };

    val fetches: List (ContractId Mod:T) -> Update (List Mod:T) =
      \(cids: List (ContractId Mod:T)) ->
        case cids of
          Cons h t ->
            ubind
              first: Mod:T <- fetch_template @Mod:T h;
              rest: List Mod:T <- Mod:fetches t
            in
              upure @(List Mod:T) Cons @Mod:T [first] rest
        | Nil ->
            upure @(List Mod:T) Nil @Mod:T;

    template (this : T) =  {
      precondition True;
      signatories Mod:T {signatories} this;
      observers Mod:T {observers} this;
      agreement "Agreement";
      choice @nonConsuming NoOp (self) (arg: Mod:NoOpArg): Unit,
        controllers Mod:NoOpArg {controllers} arg,
        observers Mod:NoOpArg {observers} arg
        to
          upure @Unit ();
     };
  }
"""

  val pkgs = SpeedyTestLib.typeAndCompile(pkg)

  def eval(
      limits: interpretation.Limits,
      contracts: PartialFunction[Value.ContractId, Versioned[Value.ContractInstance]],
      committers: Set[Ref.Party],
      e: Ast.Expr,
      agrs: SValue*
  ): Either[SError.SError, SubmittedTransaction] =
    SpeedyTestLib.buildTransaction(
      machine = Speedy.Machine.fromUpdateSExpr(
        compiledPackages = pkgs,
        transactionSeed = txSeed,
        updateSE = SExpr.SEApp(pkgs.compiler.unsafeCompile(e), agrs.view.toArray),
        committers = committers,
        limits = limits,
      ),
      getContract = contracts,
    )

  "Machine" - {

    val committers = (0 to 100).view.map(i => Ref.Party.assertFromString(s"Parties$i")).toSet
    val limit = 10
    val testCases =
      Table(
        "size" -> "success",
        1 -> true,
        limit -> true,
        limit + 1 -> false,
        5 * limit -> false,
      )

    "refuse to create a contract with too many signatories" in {
      val limits = interpretation.Limits.Lenient.copy(contractSignatories = limit)

      val e =
        e"""\(signatories: List Party) (observers: List Party) ->
         create @Mod:T Mod:T { signatories = signatories, observers = observers }
     """
      forEvery(testCases) { (i, succeed) =>
        val (signatories, observers) = committers.splitAt(i)
        val result =
          eval(limits, Map.empty, committers, e, asSParties(signatories), asSParties(observers))

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractSignatories(
                          _,
                          templateId,
                          _,
                          parties,
                          reportedlimit,
                        )
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe signatories
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to fetch a contract with too many signatories" in {
      val limits = interpretation.Limits.Lenient.copy(contractSignatories = limit)
      val e = e"""\(cid: ContractId Mod:T) -> fetch_template @Mod:T cid"""

      forEvery(testCases) { (i, succeed) =>
        val (signatories, observers) = committers.splitAt(i)
        val contract = mkContract(signatories, observers)
        val result =
          eval(limits, Map(aCid -> contract), signatories, e, SValue.SContractId(aCid))
        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractSignatories(
                          _,
                          templateId,
                          _,
                          parties,
                          reportedlimit,
                        )
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe signatories
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to exercise a contract with too many signatories" in {
      val limits = interpretation.Limits.Lenient.copy(contractSignatories = limit)
      val e =
        e"""\(cid: ContractId Mod:T) (controllers: List Party) ->
       exercise @Mod:T NoOp cid Mod:NoOpArg {controllers = controllers, observers = Nil @Party }"""

      forEvery(testCases) { (i, succeed) =>
        val (signatories, observers) = committers.splitAt(i)
        val contract = mkContract(signatories, observers)
        val result = eval(
          limits,
          Map(aCid -> contract),
          signatories,
          e,
          SValue.SContractId(aCid),
          asSParties(signatories),
        )

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractSignatories(
                          _,
                          templateId,
                          _,
                          parties,
                          reportedlimit,
                        )
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe signatories
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to create a contract with too many observers" in {
      val limits = interpretation.Limits.Lenient.copy(contractObservers = limit)

      val e =
        e"""\(signatories: List Party) (observers: List Party) ->
         create @Mod:T Mod:T { signatories = signatories, observers = observers }
     """

      forEvery(testCases) { (i, succeed) =>
        val (observers, signatories) = committers.splitAt(i)
        val result =
          eval(
            limits,
            Map.empty,
            signatories,
            e,
            asSParties(signatories),
            asSParties(observers),
          )

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractObservers(_, templateId, _, parties, reportedlimit)
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe observers
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to fetch a contract with too many observers" in {
      val limits = interpretation.Limits.Lenient.copy(contractObservers = limit)
      val e = e"""\(cid: ContractId Mod:T) -> fetch_template @Mod:T cid"""

      forEvery(testCases) { (i, succeed) =>
        val (observers, signatories) = committers.splitAt(i)
        val contract = mkContract(signatories, observers)
        val result =
          eval(limits, Map(aCid -> contract), signatories, e, SValue.SContractId(aCid))

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractObservers(_, templateId, _, parties, reportedlimit)
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe observers
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to exercise a contract with too many observers" in {
      val limits = interpretation.Limits.Lenient.copy(contractObservers = limit)
      val e =
        e"""\(cid: ContractId Mod:T) (controllers: List Party) ->
       exercise @Mod:T NoOp cid Mod:NoOpArg {controllers = controllers, observers = Nil @Party }"""

      forEvery(testCases) { (i, succeed) =>
        val (observers, signatories) = committers.splitAt(i)
        val contract = mkContract(signatories, observers)
        val result = eval(
          limits,
          Map(aCid -> contract),
          signatories,
          e,
          SValue.SContractId(aCid),
          asSParties(signatories),
        )

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ContractObservers(_, templateId, _, parties, reportedlimit)
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              parties shouldBe observers
              reportedlimit shouldBe limit
          }
      }
    }

    "refuse to exercise a choice with too many controllers" in {
      val limits = interpretation.Limits.Lenient.copy(choiceControllers = limit)
      val e =
        e"""\(signatories: List Party) (controllers: List Party) ->
        ubind
           cid: ContractId Mod:T <- create @Mod:T Mod:T {
             signatories = signatories,
             observers = Nil @Party
           }
        in exercise @Mod:T NoOp cid Mod:NoOpArg {controllers = controllers, observers = Nil @Party }
     """

      forEvery(testCases) { (i, succeed) =>
        val (controllers, signatories) = committers.splitAt(i)
        val result =
          eval(
            limits,
            Map.empty,
            committers,
            e,
            asSParties(signatories),
            asSParties(controllers),
          )

        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ChoiceControllers(
                          _,
                          templateId,
                          choiceName,
                          _,
                          parties,
                          reportedlimit,
                        )
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              choiceName shouldBe "NoOp"
              parties shouldBe controllers
              reportedlimit shouldBe limit
          }
      }
    }

    // TODO: https://github.com/digital-asset/daml/issues/15882
    // -- Add a similar test for "too many choice authorizers"
    "refuse to exercise a choice with too many observers" in {
      val limits = interpretation.Limits.Lenient.copy(choiceObservers = limit)
      val committers = (0 to 99).view.map(i => Ref.Party.assertFromString(s"Party$i")).toSet
      val e =
        e"""\(signatories: List Party) (controllers: List Party) (observers: List Party) ->
        ubind
           cid: ContractId Mod:T <- create @Mod:T Mod:T {
             signatories = signatories,
             observers = Nil @Party
           }
        in exercise @Mod:T NoOp cid Mod:NoOpArg {controllers = controllers, observers = observers}
     """

      forEvery(testCases) { (i, succeed) =>
        val (observers, signatories) = committers.splitAt(i)
        val result = eval(
          limits,
          Map.empty,
          committers,
          e,
          asSParties(signatories),
          asSParties(signatories),
          asSParties(observers),
        )
        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(
                        IE.Dev.Limit.ChoiceObservers(
                          _,
                          templateId,
                          choiceName,
                          _,
                          parties,
                          reportedlimit,
                        )
                      ),
                    )
                  )
                ) =>
              templateId shouldBe T
              choiceName shouldBe "NoOp"
              parties shouldBe observers
              reportedlimit shouldBe limit
              false
          }
      }
    }

    "refuse to build a transaction with too many input contracts" in {
      val limits = interpretation.Limits.Lenient.copy(transactionInputContracts = limit)

      val signatories = committers.take(1)
      val contract = mkContract(signatories, Set.empty)
      val cids =
        (1 to 99).map(i => Value.ContractId.V1(crypto.Hash.hashPrivateKey(s"contract$i")))
      val e = e"Mod:fetches"

      forEvery(testCases) { (i, succeed) =>
        val result = eval(limits, _ => contract, committers, e, asSCids(cids.take(i)))
        if (succeed)
          result shouldBe a[Right[_, _]]
        else
          inside(result) {
            case Left(
                  SError.SErrorDamlException(
                    IE.Dev(
                      _,
                      IE.Dev.Limit(IE.Dev.Limit.TransactionInputContracts(reportedlimit)),
                    )
                  )
                ) =>
              reportedlimit shouldBe limit
              false
          }
      }
    }
  }

  private def asSParties(parties: Iterable[Ref.Party]) =
    SValue.SList(parties.map(SValue.SParty).to(FrontStack))

  private def asSCids(cids: Iterable[Value.ContractId]) =
    SValue.SList(cids.map(SValue.SContractId).to(FrontStack))

  private val txSeed = crypto.Hash.hashPrivateKey(this.getClass.getCanonicalName)

  private[this] val T = { val Ast.TTyCon(t) = t"Mod:T"; t }

  private[this] val aCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("a contract ID"))
  private[this] def mkContract(signatories: Iterable[Ref.Party], observers: Iterable[Ref.Party]) = {

    Versioned(
      TransactionVersion.StableVersions.max,
      Value.ContractInstance(
        pkg.name,
        T,
        Value.ValueRecord(
          None,
          ImmArray(
            None -> Value.ValueList(signatories.view.map(Value.ValueParty).to(FrontStack)),
            None -> Value.ValueList(observers.view.map(Value.ValueParty).to(FrontStack)),
          ),
        ),
      ),
    )
  }
}
