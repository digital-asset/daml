// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.engine.{Error => EE}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.SExpr.SEImportValue
import com.digitalasset.daml.lf.speedy.Speedy.Machine
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Inside.inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, ParallelTestExecution}

import scala.annotation.nowarn
import scala.collection.immutable
import scala.language.implicitConversions

/** A test suite for smart contract upgrades.
  *
  * It tests many scenarios of the following form in a systematic way:
  *   - define a v1 template
  *   - define a v2 template that changes one aspect of the v1 template (e.g. observers)
  *   - create a v1 contract (locally, globally, or as a disclosure)
  *   - perform some v2 operation on the v1 contract (exercise, exercise by key, fetch, etc.)
  *   - compare the execution result to the expected outcome
  *
  * Pairs of v1/v2 templates are called [[TestCase]]s and are listed in [[testCases]]. A test case is defined by
  * subclassing [[TestCase]] and overriding one definition. For instance, [[ChangedObservers]] overrides
  * [[TestCase.v2Observers]] with an expression that is different from [[TestCase.v1Observers]].
  * Each test case is tested many times against the cartesian product of the following features:
  *  - The operation to perform ([[Exercise]], [[ExerciseByKey]], etc.), listed in [[operations]].
  *  - Whether or not to try and catch exceptions thrown when performing the operation, listed in [[catchBehaviors]].
  *  - What triggered the operation: a toplevel command or the body of a choice, listed in [[entryPoints]].
  *  - The origin of the contract being operated on: locally created, globally created, or disclosed. This is listed in
  *    [[contractOrigins]].
  *
  * Some combinations of these features don't make sense. For instance, there are no commands for fetching a contract.
  * These invalid combinations are discarded in [[TestHelper.makeApiCommands]]. For other valid combinations, this method
  * produces a command to be run against an engine by the main test loop.
  *
  * In order to test scenarios where the operation is triggered by a choice body, we need a "client" contract whose only
  * role is to exercise/fetch/lookup the v1 contract. The template for that client contract is defined in [[clientPkg]].
  * It defines a large number of choices: one per combination of operation, catch behavior, entry point and test case.
  * These choices are defined in [[TestCase.clientChoices]]. This method is pretty boilerplate-y and constitutes the
  * bulk of this test suite.
  *
  * Finally, some definitions need to be shared between v1 and v2 templates: key and interface definitions, gobal
  * parties. These are defined in [[commonDefsPkg]].
  */
class UpgradeTest extends AnyFreeSpec with Matchers with ParallelTestExecution {

  import UpgradeTest._

  def assertResultMatchesExpectedOutcome(
      result: Either[Error, (SubmittedTransaction, Transaction.Metadata)],
      expectedOutcome: ExpectedOutcome,
  ): Assertion = {
    expectedOutcome match {
      case ExpectSuccess =>
        result shouldBe a[Right[_, _]]
      case ExpectUpgradeError =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.Upgrade]
        }
      case ExpectPreprocessingError =>
        inside(result) { case Left(error) =>
          error shouldBe a[EE.Preprocessing]
        }
      case ExpectPreconditionViolated =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.TemplatePreconditionViolated]
        }
      case ExpectUnhandledException =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.UnhandledException]
        }
      case ExpectInternalInterpretationError =>
        inside(result) { case Left(EE.Interpretation(error, _)) =>
          error shouldBe a[EE.Interpretation.Internal]
        }
    }
  }

  // This is the main loop of the test: for every combination of test case, operation, catch behavior, entry point, and
  // contract origin, we generate an API command, execute it, and check that the result matches the expected outcome.
  for (testCase <- testCases)
    testCase.templateName - {
      val testHelper = new TestHelper(testCase)
      for (operation <- operations) {
        operation.name - {
          for (catchBehavior <- catchBehaviors)
            catchBehavior.name - {
              for (entryPoint <- entryPoints) {
                entryPoint.name - {
                  for (contractOrigin <- contractOrigins) {
                    contractOrigin.name - {
                      testHelper
                        .makeApiCommands(
                          operation,
                          catchBehavior,
                          entryPoint,
                          contractOrigin,
                        )
                        .foreach { apiCommands =>
                          testCase.expectedOutcome.description in {
                            assertResultMatchesExpectedOutcome(
                              testHelper.execute(apiCommands, contractOrigin),
                              testCase.expectedOutcome,
                            )
                          }
                        }
                    }
                  }
                }
              }
            }
        }
      }
    }
}

object UpgradeTest {

  val languageVersion: LanguageVersion =
    LanguageVersion.StableVersions(LanguageMajorVersion.V2).max

  private[this] implicit def parserParameters(implicit
      pkgId: PackageId
  ): ParserParameters[this.type] =
    ParserParameters(
      pkgId,
      languageVersion = languageVersion,
    )

  // implicit conversions from strings to names and qualified names
  implicit def toName(s: String): Name = Name.assertFromString(s)
  implicit def toQualifiedName(s: String): QualifiedName = QualifiedName.assertFromString(s)

  val stablePackages =
    com.digitalasset.daml.lf.stablepackages.StablePackages(LanguageMajorVersion.V2)

  val tuple2TyCon: String = {
    import stablePackages.Tuple2
    s"'${Tuple2.packageId}':${Tuple2.qualifiedName}"
  }

  // A package that defines an interface, a key type, an exception, and a party to be used by
  // the packages defined below.
  val commonDefsPkgId = PackageId.assertFromString("-common-defs-id-")
  val commonDefsPkg =
    p"""metadata ( '-common-defs-' : '1.0.0' )
          module Mod {
            record @serializable MyView = { value : Int64 };
            interface (this : Iface) = {
              viewtype Mod:MyView;

              method interfaceChoiceControllers : List Party;
              method interfaceChoiceObservers : List Party;

              choice @nonConsuming InterfaceChoice (self) (u: Unit): Text
                  , controllers (call_method @Mod:Iface interfaceChoiceControllers this)
                  , observers (call_method @Mod:Iface interfaceChoiceObservers this)
                  to upure @Text "InterfaceChoice was called";
            };

            record @serializable Ex = { message: Text } ;
            exception Ex = {
              message \(e: Mod:Ex) -> Mod:Ex {message} e
            };

            val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
            val alice : Party = Mod:mkParty "Alice";
            val bob : Party = Mod:mkParty "Bob";
          }
      """ (parserParameters(commonDefsPkgId))

  sealed abstract class ExpectedOutcome(val description: String)
  case object ExpectSuccess extends ExpectedOutcome("should succeed")
  case object ExpectPreconditionViolated
      extends ExpectedOutcome("should fail with a precondition violated error")
  case object ExpectUpgradeError extends ExpectedOutcome("should fail with an upgrade error")
  case object ExpectPreprocessingError
      extends ExpectedOutcome("should fail with a preprocessing error")
  case object ExpectUnhandledException
      extends ExpectedOutcome("should fail with an unhandled exception")
  case object ExpectInternalInterpretationError
      extends ExpectedOutcome("should fail with an internal interpretation error")

  /** Represents an LF value specified both as some string we can splice into the textual representation of LF, and as a
    * scala value we can splice into an [[ApiCommand]].
    */
  case class TestCaseValue[A](inChoiceBodyLfCode: String, inApiCommand: A)

  /** An abstract class whose [[v1TemplateDefinition]], [[v2TemplateDefinition]] and [[clientChoices]] methods generate
    * LF code that define a template named [[templateName]] and test choices for that template.
    * The class is meant to be extended by concrete case objects which override some aspect of the default template,
    * for instance [[v2Observers]].
    */
  abstract class TestCase(val templateName: String, val expectedOutcome: ExpectedOutcome) {
    def v1AdditionalDefinitions: String = ""
    def v1ChoiceArgTypeDef: String = "{}"
    def v1AdditionalFields: String = ""
    def v1AdditionalKeyFields: String = ""
    def v1AdditionalChoices: String = ""
    def v1Precondition: String = "True"
    def v1Signatories: String = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    def v1Observers: String = "Nil @Party"
    def v1Key: String =
      s"""
         |  Mod:${templateName}Key {
         |    label = "test-key",
         |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |  }""".stripMargin
    def v1Maintainers: String =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers} key)"
    def v1InterfaceChoiceControllers: String =
      s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    def v1InterfaceChoiceObservers: String = "Nil @Party"
    def v1View: String = s"'$commonDefsPkgId':Mod:MyView { value = 0 }"

    def v2AdditionalDefinitions: String = v1AdditionalDefinitions
    def v2ChoiceArgTypeDef: String = v1ChoiceArgTypeDef
    def v2AdditionalFields: String = ""
    def v2AdditionalKeyFields: String = v1AdditionalKeyFields
    def v2AdditionalChoices: String = ""
    def v2Precondition: String = v1Precondition
    def v2Signatories: String = v1Signatories
    def v2Observers: String = v1Observers
    def v2Key: String = v1Key
    def v2InterfaceChoiceControllers: String = v1InterfaceChoiceControllers
    def v2InterfaceChoiceObservers: String = v1InterfaceChoiceObservers
    def v2Maintainers: String = v1Maintainers
    def v2View: String = v1View

    // Used for creating contracts in choice bodies
    def additionalCreateArgsLf(v1PkgId: PackageId): String = ""
    // Used for creating contracts in commands
    def additionalCreateArgsValue(@nowarn v1PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray.empty

    // Used for creating disclosures of v1 contracts and the "lookup contract by key" map passed to the engine.
    def additionalv1KeyArgsValue(@nowarn v1PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray.empty
    // Used for creating *ByKey commands
    def additionalv2KeyArgsValue(@nowarn v2PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray.empty
    // Used for looking up contracts by key in choice bodies
    def additionalv2KeyArgsLf(v2PkgId: PackageId): String = ""

    // Used for choice args in commands
    def choiceArgValue: ImmArray[(Option[Name], Value)] = ImmArray.empty

    def templateDefinition(
        additionalDefinitions: String,
        choiceArgTypeDef: String,
        additionalFields: String,
        additionalKeyFields: String,
        additionalChoices: String,
        precondition: String,
        signatories: String,
        observers: String,
        key: String,
        maintainers: String,
        interfaceChoiceControllers: String,
        interfaceChoiceObservers: String,
        view: String,
    ): String =
      s"""
         |  $additionalDefinitions
         |
         |  record @serializable ${templateName}Key =
         |    { label: Text
         |    , maintainers: List Party
         |    $additionalKeyFields
         |    };
         |
         |  record @serializable ${templateName}ChoiceArgType = $choiceArgTypeDef;
         |
         |  record @serializable $templateName =
         |    { p1: Party
         |    , p2: Party
         |    $additionalFields
         |    };
         |
         |  template (this: $templateName) = {
         |    precondition $precondition;
         |    signatories $signatories;
         |    observers $observers;
         |
         |    choice @nonConsuming TemplateChoice (self) (u: Mod:${templateName}ChoiceArgType): Text
         |      , controllers (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |      , observers (Nil @Party)
         |      to upure @Text "TemplateChoice was called";
         |
         |    $additionalChoices
         |
         |    implements '$commonDefsPkgId':Mod:Iface {
         |      view = $view;
         |      method interfaceChoiceControllers = $interfaceChoiceControllers;
         |      method interfaceChoiceObservers = $interfaceChoiceObservers;
         |    };
         |
         |    key @Mod:${templateName}Key ($key) ($maintainers);
         |  };""".stripMargin

    def v1TemplateDefinition: String = templateDefinition(
      v1AdditionalDefinitions,
      v1ChoiceArgTypeDef,
      v1AdditionalFields,
      v1AdditionalKeyFields,
      v1AdditionalChoices,
      v1Precondition,
      v1Signatories,
      v1Observers,
      v1Key,
      v1Maintainers,
      v1InterfaceChoiceControllers,
      v1InterfaceChoiceObservers,
      v1View,
    )

    def v2TemplateDefinition: String = templateDefinition(
      v2AdditionalDefinitions,
      v2ChoiceArgTypeDef,
      v2AdditionalFields,
      v2AdditionalKeyFields,
      v2AdditionalChoices,
      v2Precondition,
      v2Signatories,
      v2Observers,
      v2Key,
      v2Maintainers,
      v2InterfaceChoiceControllers,
      v2InterfaceChoiceObservers,
      v2View,
    )

    def clientChoices(
        clientPkgId: PackageId,
        v1PkgId: PackageId,
        v2PkgId: PackageId,
    ): String = {
      val clientTplQualifiedName = s"'$clientPkgId':Mod:Client"
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      val ifaceQualifiedName = s"'$commonDefsPkgId':Mod:Iface"
      val v2KeyTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}Key"
      val v2ChoiceArgTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}ChoiceArgType"

      val createV1ContractExpr =
        s""" create
           |    @$v1TplQualifiedName
           |    ($v1TplQualifiedName
           |        { p1 = '$commonDefsPkgId':Mod:alice
           |        , p2 = '$commonDefsPkgId':Mod:bob
           |        ${additionalCreateArgsLf(v1PkgId)}
           |        })
           |""".stripMargin

      val v2KeyExpr =
        s""" ($v2KeyTypeQualifiedName
           |     { label = "test-key"
           |     , maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party))
           |     ${additionalv2KeyArgsLf(v2PkgId)}
           |     })""".stripMargin

      val choiceArgExpr = s"($v2ChoiceArgTypeQualifiedName {})"

      s"""
         |  choice @nonConsuming ExerciseNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in exercise
         |            @$v2TplQualifiedName
         |            TemplateChoice
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
         |            $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseNoCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise
         |         @$v2TplQualifiedName
         |         TemplateChoice
         |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
         |         $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseAttemptCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $v1TplQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseInterfaceNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in exercise_interface
         |            @$ifaceQualifiedName
         |            InterfaceChoice
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |            ();
         |
         |  choice @nonConsuming ExerciseInterfaceAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseInterfaceNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseInterfaceNoCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $v1TplQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise_interface
         |         @$ifaceQualifiedName
         |         InterfaceChoice
         |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |         ();
         |
         |  choice @nonConsuming ExerciseInterfaceAttemptCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $v1TplQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseInterfaceNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseByKeyNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in exercise_by_key
         |            @$v2TplQualifiedName
         |            TemplateChoice
         |            $v2KeyExpr
         |            $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseByKeyNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise_by_key
         |         @$v2TplQualifiedName
         |         TemplateChoice
         |         key
         |         $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <- exercise @$clientTplQualifiedName ExerciseByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchNoCatchLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in fetch_template
         |            @$v2TplQualifiedName
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid);
         |
         |  choice @nonConsuming FetchAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchNoCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName)
         |        : $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to fetch_template
         |         @$v2TplQualifiedName
         |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid);
         |
         |  choice @nonConsuming FetchAttemptCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchByKeyNoCatchLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in ubind pair:$tuple2TyCon (ContractId $v2TplQualifiedName) $v2TplQualifiedName <-
         |              fetch_by_key
         |                @$v2TplQualifiedName
         |                $v2KeyExpr
         |          in upure @$v2TplQualifiedName ($tuple2TyCon @(ContractId $v2TplQualifiedName) @$v2TplQualifiedName {_2} pair);
         |
         |  choice @nonConsuming FetchByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchByKeyNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind pair:$tuple2TyCon (ContractId $v2TplQualifiedName) $v2TplQualifiedName <-
         |          fetch_by_key
         |            @$v2TplQualifiedName
         |            key
         |       in upure @$v2TplQualifiedName ($tuple2TyCon @(ContractId $v2TplQualifiedName) @$v2TplQualifiedName {_2} pair);
         |
         |  choice @nonConsuming FetchByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchInterfaceNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind
         |         cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr;
         |         iface: $ifaceQualifiedName <- fetch_interface
         |            @$ifaceQualifiedName
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |         in upure @Text "no exception was caught";
         |
         |  choice @nonConsuming FetchInterfaceAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <-
         |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchInterfaceNoCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind iface: $ifaceQualifiedName <- fetch_interface
         |         @$ifaceQualifiedName
         |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |       in upure @Text "no exception was caught";
         |
         |  choice @nonConsuming FetchInterfaceAttemptCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Text <-
         |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming LookupByKeyNoCatchLocal${templateName} (self) (u: Unit): Option (ContractId $v2TplQualifiedName)
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in lookup_by_key
         |            @$v2TplQualifiedName
         |            $v2KeyExpr;
         |
         |  choice @nonConsuming LookupByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Option (ContractId $v2TplQualifiedName) <-
         |             exercise @$clientTplQualifiedName LookupByKeyNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming LookupByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Option (ContractId $v2TplQualifiedName)
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to lookup_by_key
         |         @$v2TplQualifiedName
         |         key;
         |
         |  choice @nonConsuming LookupByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind _:Option (ContractId $v2TplQualifiedName) <-
         |             exercise @$clientTplQualifiedName LookupByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |""".stripMargin
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed precondition expression, evaluates to true, upgrade succeeds
  case object UnchangedPrecondition extends TestCase("UnchangedPrecondition", ExpectSuccess) {
    override def v1Precondition = "True"
    override def v2Precondition = "case () of () -> True"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed precondition expression, evaluates to false, upgrade fails
  case object ChangedPrecondition
      extends TestCase("ChangedPrecondition", ExpectPreconditionViolated) {
    override def v1Precondition = "True"
    override def v2Precondition = "False"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the precondition expressions cannot be caught
  case object ThrowingPrecondition
      extends TestCase("ThrowingPrecondition", ExpectUnhandledException) {
    override def v1Precondition = "True"
    override def v2Precondition =
      s"""throw @Bool @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Precondition"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed signatories expression, evaluates to the same value, upgrade succeeds
  case object UnchangedSignatories extends TestCase("UnchangedSignatories", ExpectSuccess) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"case () of () -> Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed signatories expression, evaluates to a different value, upgrade fails
  case object ChangedSignatories extends TestCase("ChangedSignatories", ExpectUpgradeError) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"Cons @Party [Mod:${templateName} {p1} this, Mod:${templateName} {p2} this] (Nil @Party)"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the signatories expression cannot be caught
  case object ThrowingSignatories
      extends TestCase("ThrowingSignatories", ExpectUnhandledException) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Signatories"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed observers expression, evaluates to the same value, upgrade succeeds
  case object UnchangedObservers extends TestCase("UnchangedObservers", ExpectSuccess) {
    override def v1Observers = "Nil @Party"
    override def v2Observers = "case () of () -> Nil @Party"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed observers expression, evaluates to a different value, upgrade fails
  case object ChangedObservers extends TestCase("ChangedObservers", ExpectUpgradeError) {
    override def v1Observers = "Nil @Party"
    override def v2Observers = s"Cons @Party [Mod:${templateName} {p2} this] (Nil @Party)"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the observers expression cannot be caught
  case object ThrowingObservers extends TestCase("ThrowingObservers", ExpectUnhandledException) {
    override def v1Observers = "Nil @Party"
    override def v2Observers =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Observers"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: the new version of the template defines an additional choice, upgrade succeeds
  case object AdditionalChoices extends TestCase("AdditionalChoices", ExpectSuccess) {
    override def v1AdditionalChoices: String = ""
    override def v2AdditionalChoices: String =
      s"""
         | choice @nonConsuming AdditionalChoice (self) (u: Unit): Text
         |   , controllers (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |   , observers (Nil @Party)
         |   to upure @Text "AdditionalChoice was called";
         |""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: the new version of the module defines an additional template, upgrade succeeds
  case object AdditionalTemplates extends TestCase("AdditionalTemplates", ExpectSuccess) {
    override def v1AdditionalDefinitions = ""
    override def v2AdditionalDefinitions =
      s"""
         | record @serializable ${templateName}AdditionalTemplate = { p : Party };
         | template (this: ${templateName}AdditionalTemplate) = {
         |   precondition True;
         |   signatories Cons @Party [Mod:${templateName}AdditionalTemplate {p} this] (Nil @Party);
         |   observers Nil @Party;
         | };
         |""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed key expression, evaluates to the same value, upgrade succeeds
  case object UnchangedKey extends TestCase("UnchangedKey", ExpectSuccess) {
    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s""" case () of () ->
                            |    Mod:${templateName}Key {
                            |      label = "test-key",
                            |      maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |    }""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed key expression, evaluates to a different value, upgrade fails
  case object ChangedKey extends TestCase("ChangedKey", ExpectUpgradeError) {
    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s"""
                            |    Mod:${templateName}Key {
                            |      label = "test-key-2",
                            |      maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |    }""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the key expression cannot be caught
  case object ThrowingKey extends TestCase("ThrowingKey", ExpectUnhandledException) {
    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key =
      s"""throw @Mod:${templateName}Key @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Key"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed maintainers expression, evaluates to the same value, upgrade succeeds
  case object UnchangedMaintainers extends TestCase("UnchangedMaintainers", ExpectSuccess) {
    override def v1Maintainers =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers} key)"
    override def v2Maintainers =
      s"\\(key: Mod:${templateName}Key) -> case () of () -> (Mod:${templateName}Key {maintainers} key)"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: changed maintainers expression, evaluates to a different value, upgrade fails
  case object ChangedMaintainers extends TestCase("ChangedMaintainers", ExpectUpgradeError) {
    override def v1AdditionalKeyFields: String = ", maintainers2: List Party"
    override def v2AdditionalKeyFields: String = v1AdditionalKeyFields

    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = v1Key

    // Used for creating disclosures of v1 contracts and the "lookup contract by key" map passed to the engine.
    override def additionalv1KeyArgsValue(pkgId: PackageId) =
      ImmArray(
        Some("maintainers2": Name) -> ValueList(
          FrontStack(ValueParty(Party.assertFromString("Bob")))
        )
      )
    // Used for creating *ByKey commands
    override def additionalv2KeyArgsValue(pkgId: PackageId) =
      additionalv1KeyArgsValue(pkgId)
    // Used for looking up contracts by key in choice bodies
    override def additionalv2KeyArgsLf(v2PkgId: PackageId) =
      s", maintainers2 = (Cons @Party ['$commonDefsPkgId':Mod:bob] (Nil @Party))"

    override def v1Maintainers =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers} key)"
    override def v2Maintainers =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers2} key)"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the maintainers expression cannot be caught
  case object ThrowingMaintainers
      extends TestCase("ThrowingMaintainers", ExpectUnhandledException) {
    override def v1Maintainers =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers} key)"
    override def v2Maintainers =
      s"""throw @(Mod:${templateName}Key -> List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Maintainers"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the maintainers function body cannot be caught
  case object ThrowingMaintainersBody
      extends TestCase("ThrowingMaintainersBody", ExpectUnhandledException) {
    override def v1Maintainers =
      s"\\(key: Mod:${templateName}Key) -> (Mod:${templateName}Key {maintainers} key)"
    override def v2Maintainers =
      s"""\\(key: Mod:${templateName}Key) -> throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "MaintainersBody"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as choice parameter, upgrade succeeds
  case object AdditionalFieldInChoiceArgRecordField
      extends TestCase("AdditionalFieldInChoiceArgRecordField", ExpectSuccess) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"

    override def v1ChoiceArgTypeDef = s"{ r : Mod:$recordName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("r": Name) -> ValueRecord(
          None,
          ImmArray(
            Some("n": Name) -> ValueInt64(0),
            Some("extra": Name) -> ValueOptional(Some(ValueUnit)),
          ),
        )
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as choice parameter, None value, downgrade succeeds
  case object ValidDowngradeAdditionalFieldInChoiceArgRecordField
      extends TestCase("ValidDowngradeAdditionalFieldInChoiceArgRecordField", ExpectSuccess) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"

    override def v1ChoiceArgTypeDef = s"{ r : Mod:$recordName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("r": Name) -> ValueRecord(
          None,
          ImmArray(
            Some("n": Name) -> ValueInt64(0),
            Some("extra": Name) -> ValueOptional(None),
          ),
        )
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as choice parameter, Some value, downgrade fails
  case object InvalidDowngradeAdditionalFieldInChoiceArgRecordField
      extends TestCase(
        "InvalidDowngradeAdditionalFieldInChoiceArgRecordField",
        ExpectPreprocessingError,
      ) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"

    override def v1ChoiceArgTypeDef = s"{ r : Mod:$recordName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("r": Name) -> ValueRecord(
          None,
          ImmArray(
            Some("n": Name) -> ValueInt64(0),
            Some("extra": Name) -> ValueOptional(Some(ValueUnit)),
          ),
        )
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as choice parameter, upgrade succeeds
  case object AdditionalConstructorInChoiceArgVariantField
      extends TestCase("AdditionalConstructorInChoiceArgVariantField", ExpectSuccess) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"

    override def v1ChoiceArgTypeDef = s"{ v : Mod:$variantName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("v": Name) -> ValueVariant(None, "Ctor2", ValueUnit)
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as choice parameter, unused constructor, downgrade succeeds
  case object ValidDowngradeAdditionalConstructorInChoiceArgVariantField
      extends TestCase(
        "ValidDowngradeAdditionalConstructorInChoiceArgVariantField",
        ExpectSuccess,
      ) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"

    override def v1ChoiceArgTypeDef = s"{ v : Mod:$variantName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("v": Name) -> ValueVariant(None, "Ctor1", ValueInt64(0))
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as choice parameter, used constructor, downgrade fails
  case object InvalidDowngradeAdditionalConstructorInChoiceArgVariantField
      extends TestCase(
        "InvalidDowngradeAdditionalConstructorInChoiceArgVariantField",
        ExpectPreprocessingError,
      ) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"

    override def v1ChoiceArgTypeDef = s"{ v : Mod:$variantName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("v": Name) -> ValueVariant(None, "Ctor2", ValueUnit)
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as choice parameter, upgrade succeeds
  case object AdditionalConstructorInChoiceArgEnumField
      extends TestCase("AdditionalConstructorInChoiceArgEnumField", ExpectSuccess) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"

    override def v1ChoiceArgTypeDef = s"{ e : Mod:$enumName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("e": Name) -> ValueEnum(None, "Ctor2")
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as choice parameter, unused constructor, downgrade succeeds
  case object ValidDowngradeAdditionalConstructorInChoiceArgEnumField
      extends TestCase("ValidDowngradeAdditionalConstructorInChoiceArgEnumField", ExpectSuccess) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"

    override def v1ChoiceArgTypeDef = s"{ e : Mod:$enumName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("e": Name) -> ValueEnum(None, "Ctor1")
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as choice parameter, used constructor, downgrade fails
  case object InvalidDowngradeAdditionalConstructorInChoiceArgEnumField
      extends TestCase(
        "InvalidDowngradeAdditionalConstructorInChoiceArgEnumField",
        ExpectPreprocessingError,
      ) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"

    override def v1ChoiceArgTypeDef = s"{ e : Mod:$enumName }"
    override def v2ChoiceArgTypeDef = v1ChoiceArgTypeDef

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("e": Name) -> ValueEnum(None, "Ctor2")
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional choice parameter, upgrade succeeds
  case object AdditionalChoiceArg extends TestCase("AdditionalChoiceArg", ExpectSuccess) {

    override def v1ChoiceArgTypeDef = s"{ n : Int64 }"
    override def v2ChoiceArgTypeDef = s"{ n : Int64, extra: Option Unit }"

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("n": Name) -> ValueInt64(0)
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional choice parameter, None arg, downgrade succeeds
  case object ValidDowngradeAdditionalChoiceArg
      extends TestCase("ValidDowngradeAdditionalChoiceArg", ExpectSuccess) {

    override def v1ChoiceArgTypeDef = s"{ n : Int64, extra: Option Unit  }"
    override def v2ChoiceArgTypeDef = s"{ n : Int64 }"

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("n": Name) -> ValueInt64(0),
        Some("extra": Name) -> ValueOptional(None),
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional choice parameter, Some arg, downgrade fails
  case object InvalidDowngradeAdditionalChoiceArg
      extends TestCase("InvalidDowngradeAdditionalChoiceArg", ExpectPreprocessingError) {

    override def v1ChoiceArgTypeDef = s"{ n : Int64, extra: Option Unit  }"
    override def v2ChoiceArgTypeDef = s"{ n : Int64 }"

    override def choiceArgValue: ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("n": Name) -> ValueInt64(0),
        Some("extra": Name) -> ValueOptional(Some(ValueUnit)),
      )
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as template parameter, upgrade succeeds
  case object AdditionalFieldInRecordArg
      extends TestCase("AdditionalFieldInRecordArg", ExpectSuccess) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"

    override def v1AdditionalFields = s", r: Mod:$recordName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedRecordName = s"'$v1PkgId':Mod:$recordName"
      s", r = $qualifiedRecordName { n = 0 }"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1RecordId =
        Identifier(v1PkgId, s"Mod:$recordName")
      ImmArray(
        Some("r": Name) -> ValueRecord(
          Some(v1RecordId),
          ImmArray(
            Some("n": Name) -> ValueInt64(0)
          ),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as template parameter, upgrade succeeds
  case object AdditionalConstructorInVariantArg
      extends TestCase("AdditionalConstructorInVariantArg", ExpectSuccess) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"

    override def v1AdditionalFields = s", v: Mod:$variantName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedVariantName = s"'$v1PkgId':Mod:$variantName"
      s", v = $qualifiedVariantName:Ctor1 0"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1VariantId =
        Identifier(v1PkgId, s"Mod:$variantName")
      ImmArray(
        Some("v": Name) -> ValueVariant(
          Some(v1VariantId),
          "Ctor1",
          ValueInt64(0),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as template parameter, upgrade succeeds
  case object AdditionalConstructorInEnumArg
      extends TestCase("AdditionalConstructorInEnumArg", ExpectSuccess) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"

    override def v1AdditionalFields = s", e: Mod:$enumName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedEnumName = s"'$v1PkgId':Mod:$enumName"
      s", e = $qualifiedEnumName:Ctor1"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1EnumId = Identifier(v1PkgId, s"Mod:$enumName")
      ImmArray(
        Some("e": Name) -> ValueEnum(
          Some(v1EnumId),
          "Ctor1",
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional template parameter, upgrade succeeds
  case object AdditionalTemplateArg extends TestCase("AdditionalTemplateArg", ExpectSuccess) {
    override def v1AdditionalFields = ""
    override def v2AdditionalFields = ", extra: Option Unit"
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the choice controllers expression cannot be caught
  case object ThrowingInterfaceChoiceControllers
      extends TestCase("ThrowingInterfaceChoiceControllers", ExpectUnhandledException) {
    override def v1InterfaceChoiceControllers =
      s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2InterfaceChoiceControllers =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "InterfaceChoiceControllers"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: errors thrown by the choice observers expression cannot be caught
  case object ThrowingInterfaceChoiceObservers
      extends TestCase("ThrowingInterfaceChoiceObservers", ExpectUnhandledException) {
    override def v1InterfaceChoiceObservers =
      s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2InterfaceChoiceObservers =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "InterfaceChoiceObservers"})"""
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: interface views are not calculated during fetches and exercises
  case object ThrowingView extends TestCase("ThrowingView", ExpectSuccess) {
    override def v1View = s"'$commonDefsPkgId':Mod:MyView { value = 0 }"
    override def v2View =
      s"""throw @'$commonDefsPkgId':Mod:MyView @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "View"})"""
  }

  case object ValidDowngradeAdditionalTemplateArg
      extends TestCase("ValidDowngradeAdditionalTemplateArg", ExpectSuccess) {
    override def v1AdditionalFields = ", extra: Option Unit"
    override def v2AdditionalFields = ""

    override def additionalCreateArgsLf(v1PkgId: PackageId): String =
      s", extra = None @Unit"

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        Some("extra": Name) -> ValueOptional(None)
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional template parameter, None arg, downgrade succeeds
  case object InvalidDowngradeAdditionalTemplateArg
      extends TestCase("InvalidDowngradeAdditionalTemplateArg", ExpectUpgradeError) {
    override def v1AdditionalFields = ", extra: Option Unit"
    override def v2AdditionalFields = ""

    override def additionalCreateArgsLf(v1PkgId: PackageId): String =
      s", extra = Some @Unit ()"

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        Some("extra": Name) -> ValueOptional(Some(ValueUnit))
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as template parameter, None value, downgrade succeeds
  case object ValidDowngradeAdditionalFieldInRecordArg
      extends TestCase("ValidDowngradeAdditionalFieldInRecordArg", ExpectSuccess) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"

    override def v1AdditionalFields = s", r: Mod:$recordName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedRecordName = s"'$v1PkgId':Mod:$recordName"
      s", r = $qualifiedRecordName { n = 0, extra = None @Unit }"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1RecordId =
        Identifier(v1PkgId, s"Mod:$recordName")
      ImmArray(
        Some("r": Name) -> ValueRecord(
          Some(v1RecordId),
          ImmArray(
            Some("n": Name) -> ValueInt64(0),
            Some("extra": Name) -> ValueOptional(None),
          ),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as template parameter, Some value, downgrade fails
  case object InvalidDowngradeAdditionalFieldInRecordArg
      extends TestCase("InvalidDowngradeAdditionalFieldInRecordArg", ExpectUpgradeError) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64, extra: Option Unit };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"

    override def v1AdditionalFields = s", r: Mod:$recordName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedRecordName = s"'$v1PkgId':Mod:$recordName"
      s", r = $qualifiedRecordName { n = 0, extra = Some @Unit () }"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1RecordId =
        Identifier(v1PkgId, s"Mod:$recordName")
      ImmArray(
        Some("r": Name) -> ValueRecord(
          Some(v1RecordId),
          ImmArray(
            Some("n": Name) -> ValueInt64(0),
            Some("extra": Name) -> ValueOptional(Some(ValueUnit)),
          ),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as template parameter, unused constructor, downgrade succeeds
  case object ValidDowngradeAdditionalConstructorInVariantArg
      extends TestCase("ValidDowngradeAdditionalConstructorInVariantArg", ExpectSuccess) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"

    override def v1AdditionalFields = s", v: Mod:$variantName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedVariantName = s"'$v1PkgId':Mod:$variantName"
      s", v = $qualifiedVariantName:Ctor1 0"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1VariantId =
        Identifier(v1PkgId, s"Mod:$variantName")
      ImmArray(
        Some("v": Name) -> ValueVariant(
          Some(v1VariantId),
          "Ctor1",
          ValueInt64(0),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in variant used as template parameter, used constructor, downgrade fails
  case object InvalidDowngradeAdditionalConstructorInVariantArg
      extends TestCase(
        "InvalidDowngradeAdditionalConstructorInVariantArg",
        ExpectUpgradeError,
      ) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64;"

    override def v1AdditionalFields = s", v: Mod:$variantName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedVariantName = s"'$v1PkgId':Mod:$variantName"
      s", v = $qualifiedVariantName:Ctor2 ()"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1VariantId =
        Identifier(v1PkgId, s"Mod:$variantName")
      ImmArray(
        Some("v": Name) -> ValueVariant(
          Some(v1VariantId),
          "Ctor2",
          ValueUnit,
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as template parameter, unused constructor, downgrade succeeds
  case object ValidDowngradeAdditionalConstructorInEnumArg
      extends TestCase("ValidDowngradeAdditionalConstructorInEnumArg", ExpectSuccess) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"

    override def v1AdditionalFields = s", e: Mod:$enumName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedEnumName = s"'$v1PkgId':Mod:$enumName"
      s", e = $qualifiedEnumName:Ctor1"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1EnumId = Identifier(v1PkgId, s"Mod:$enumName")
      ImmArray(
        Some("e": Name) -> ValueEnum(
          Some(v1EnumId),
          "Ctor1",
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as template parameter, used constructor, downgrade fails
  case object InvalidDowngradeAdditionalConstructorInEnumArg
      extends TestCase(
        "InvalidDowngradeAdditionalConstructorInEnumArg",
        ExpectUpgradeError,
      ) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1;"

    override def v1AdditionalFields = s", e: Mod:$enumName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedEnumName = s"'$v1PkgId':Mod:$enumName"
      s", e = $qualifiedEnumName:Ctor2"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      val v1EnumId = Identifier(v1PkgId, s"Mod:$enumName")
      ImmArray(
        Some("e": Name) -> ValueEnum(
          Some(v1EnumId),
          "Ctor2",
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in key type, None value, upgrade succeeds
  case object ValidKeyUpgradeAdditionalField
      extends TestCase("ValidKeyUpgradeAdditionalField", ExpectSuccess) {
    override def v1AdditionalKeyFields: String = ""
    override def v2AdditionalKeyFields: String = ", extra: Option Unit"

    override def additionalv2KeyArgsLf(v2PkgId: PackageId) =
      s", extra = None @Unit"
    override def additionalv2KeyArgsValue(v2PkgId: PackageId) =
      ImmArray(
        Some("extra": Name) -> ValueOptional(None)
      )

    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    extra = None @Unit
                            |  }""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in key type, Some value, upgrade fails
  case object InvalidKeyUpgradeAdditionalField
      extends TestCase("InvalidKeyUpgradeAdditionalField", ExpectUpgradeError) {
    override def v1AdditionalKeyFields: String = ""
    override def v2AdditionalKeyFields: String = ", extra: Option Unit"

    override def additionalv2KeyArgsLf(v2PkgId: PackageId) =
      s", extra = None @Unit"
    override def additionalv2KeyArgsValue(v2PkgId: PackageId) =
      ImmArray(
        Some("extra": Name) -> ValueOptional(None)
      )

    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    extra = Some @Unit ()
                            |  }""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in key type, None value, downgrade succeeds
  case object ValidKeyDowngradeAdditionalField
      extends TestCase("ValidKeyDowngradeAdditionalField", ExpectSuccess) {
    override def v1AdditionalKeyFields: String = ", extra: Option Unit"
    override def v2AdditionalKeyFields: String = ""

    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    extra = None @Unit
                            |  }""".stripMargin
    override def v2Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in key type, Some value, downgrade fails
  case object InvalidKeyDowngradeAdditionalField
      extends TestCase("InvalidKeyDowngradeAdditionalField", ExpectUpgradeError) {
    override def v1AdditionalKeyFields: String = ", extra: Option Unit"
    override def v2AdditionalKeyFields: String = ""

    // Used for creating disclosures of v1 contracts and the "lookup contract by key" map passed to the engine.
    override def additionalv1KeyArgsValue(v1PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray(
        Some("extra": Name) -> ValueOptional(Some(ValueUnit))
      )

    override def v1Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    extra = Some @Unit ()
                            |  }""".stripMargin
    override def v2Key = s"""
                            |  Mod:${templateName}Key {
                            |    label = "test-key",
                            |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
                            |  }""".stripMargin
  }

  // Test cases that apply to both commands and "in choice body" operations.
  val commandAndChoiceTestCases: Seq[TestCase] = List(
    // metadata
    UnchangedPrecondition,
    ChangedPrecondition,
    ThrowingPrecondition,
    UnchangedSignatories,
    ChangedSignatories,
    ThrowingSignatories,
    UnchangedObservers,
    ChangedObservers,
    ThrowingObservers,
    // keys and maintainers
    ChangedKey,
    UnchangedKey,
    ThrowingKey,
    ChangedMaintainers,
    UnchangedMaintainers,
    ThrowingMaintainers,
    ThrowingMaintainersBody,
    // key upgrades
    ValidKeyUpgradeAdditionalField,
    InvalidKeyUpgradeAdditionalField,
    // key downgrades
    ValidKeyDowngradeAdditionalField,
    InvalidKeyDowngradeAdditionalField,
    // template arg
    AdditionalFieldInRecordArg,
    AdditionalConstructorInVariantArg,
    AdditionalConstructorInEnumArg,
    AdditionalTemplateArg,
    // cases that test that adding unrelated stuff to the package has no impact
    AdditionalChoices,
    AdditionalTemplates,
    // template arg downgrade
    ValidDowngradeAdditionalTemplateArg,
    InvalidDowngradeAdditionalTemplateArg,
    ValidDowngradeAdditionalFieldInRecordArg,
    InvalidDowngradeAdditionalFieldInRecordArg,
    ValidDowngradeAdditionalConstructorInVariantArg,
    InvalidDowngradeAdditionalConstructorInVariantArg,
    ValidDowngradeAdditionalConstructorInEnumArg,
    InvalidDowngradeAdditionalConstructorInEnumArg,
    // interface instances
    ThrowingInterfaceChoiceControllers,
    ThrowingInterfaceChoiceObservers,
    ThrowingView,
  )

  // Test cases that only apply to commands.
  val commandOnlyTestCases: Seq[TestCase] = List(
    // choice arg
    AdditionalFieldInChoiceArgRecordField,
    AdditionalConstructorInChoiceArgVariantField,
    AdditionalConstructorInChoiceArgEnumField,
    AdditionalChoiceArg,
    // choice arg downgrade
    ValidDowngradeAdditionalFieldInChoiceArgRecordField,
    InvalidDowngradeAdditionalFieldInChoiceArgRecordField,
    ValidDowngradeAdditionalConstructorInChoiceArgVariantField,
    InvalidDowngradeAdditionalConstructorInChoiceArgVariantField,
    ValidDowngradeAdditionalConstructorInChoiceArgEnumField,
    InvalidDowngradeAdditionalConstructorInChoiceArgEnumField,
    ValidDowngradeAdditionalChoiceArg,
    InvalidDowngradeAdditionalChoiceArg,
  )

  // All test cases.
  val testCases = commandAndChoiceTestCases ++ commandOnlyTestCases;

  val templateDefsPkgName = PackageName.assertFromString("-template-defs-")

  /** A package that defines templates called Precondition, Signatories, ... whose metadata should evaluate without
    * throwing exceptions.
    */
  val templateDefsV1PkgId = PackageId.assertFromString("-template-defs-v1-id-")
  val templateDefsV1ParserParams = parserParameters(templateDefsV1PkgId)
  val templateDefsV1Pkg =
    p"""metadata ( '$templateDefsPkgName' : '1.0.0' )
          module Mod {
            ${testCases.map(_.v1TemplateDefinition).mkString("\n")}
          }
      """ (templateDefsV1ParserParams)

  /** Version 2 of the package above. It upgrades the previously defined templates such that:
    *   - the precondition in the Precondition template is changed to throw an exception
    *   - the signatories in the Signatories template is changed to throw an exception
    *   - etc.
    */
  val templateDefsV2PkgId = PackageId.assertFromString("-template-defs-v2-id-")
  val templateDefsV2ParserParams = parserParameters(templateDefsV2PkgId)
  val templateDefsV2Pkg =
    p"""metadata ( '$templateDefsPkgName' : '2.0.0' )
          module Mod {
            ${testCases.map(_.v2TemplateDefinition).mkString("\n")}
          }
      """ (templateDefsV2ParserParams)

  val clientPkgId = PackageId.assertFromString("-client-id-")
  val clientParserParams = parserParameters(clientPkgId)
  val clientPkg = {
    val choices = commandAndChoiceTestCases
      .map(_.clientChoices(clientPkgId, templateDefsV1PkgId, templateDefsV2PkgId))
    p"""metadata ( '-client-' : '1.0.0' )
            module Mod {
              record @serializable Client = { p: Party };
              template (this: Client) = {
                precondition True;
                signatories Cons @Party [Mod:Client {p} this] (Nil @Party);
                observers Nil @Party;

                ${choices.mkString("\n")}
              };
            }
        """ (clientParserParams)
  }

  val lookupPackage: Map[PackageId, Ast.Package] = Map(
    stablePackages.Tuple2.packageId -> stablePackages.packagesMap(stablePackages.Tuple2.packageId),
    commonDefsPkgId -> commonDefsPkg,
    templateDefsV1PkgId -> templateDefsV1Pkg,
    templateDefsV2PkgId -> templateDefsV2Pkg,
    clientPkgId -> clientPkg,
  )

  val packageMap: Map[PackageId, (PackageName, PackageVersion)] =
    lookupPackage.view.mapValues(pkg => (pkg.pkgName, pkg.metadata.version)).toMap

  val engineConfig: EngineConfig = EngineConfig(allowedLanguageVersions =
    language.LanguageVersion.AllVersions(LanguageMajorVersion.V2)
  )

  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(lookupPackage, engineConfig.getCompilerConfig)

  sealed abstract class Operation(val name: String)
  case object Exercise extends Operation("Exercise")
  case object ExerciseByKey extends Operation("ExerciseByKey")
  case object ExerciseInterface extends Operation("ExerciseInterface")
  case object Fetch extends Operation("Fetch")
  case object FetchByKey extends Operation("FetchByKey")
  case object FetchInterface extends Operation("FetchInterface")
  case object LookupByKey extends Operation("LookupByKey")

  val operations: List[Operation] =
    List(
      Exercise,
      ExerciseByKey,
      ExerciseInterface,
      Fetch,
      FetchByKey,
      FetchInterface,
      LookupByKey,
    )

  sealed abstract class CatchBehavior(val name: String)
  case object AttemptCatch extends CatchBehavior("AttemptCatch")
  case object NoCatch extends CatchBehavior("NoCatch")

  val catchBehaviors: List[CatchBehavior] = List(AttemptCatch, NoCatch)

  sealed abstract class EntryPoint(val name: String)
  case object Command extends EntryPoint("Command")
  case object ChoiceBody extends EntryPoint("ChoiceBody")

  val entryPoints: List[EntryPoint] = List(Command, ChoiceBody)

  sealed abstract class ContractOrigin(val name: String)
  case object Global extends ContractOrigin("Global")
  case object Disclosed extends ContractOrigin("Disclosed")
  case object Local extends ContractOrigin("Local")

  val contractOrigins: List[ContractOrigin] = List(Global, Disclosed, Local)

  /** A class that defines all the "global" variables shared by tests for a given template name: the template ID of the
    * v1 template, the template ID of the v2 template, the ID of the v1 contract, etc. It exposes two methods:
    * - [[makeApiCommands]], which generates an API command for a given operation, catch behavior, entry point,
    *   and contract origin.
    * - [[execute]], which executes a command against a fresh engine seeded with the v1 contract.
    */
  class TestHelper(testCase: TestCase) {

    implicit val logContext: LoggingContext = LoggingContext.ForTesting

    val templateName = testCase.templateName

    val alice: Party = Party.assertFromString("Alice")
    val bob: Party = Party.assertFromString("Bob")

    val clientTplId: Identifier = Identifier(clientPkgId, "Mod:Client")
    val ifaceId: Identifier = Identifier(commonDefsPkgId, "Mod:Iface")
    val tplQualifiedName: QualifiedName = s"Mod:$templateName"
    val tplRef: TypeConRef = TypeConRef.assertFromString(s"#$templateDefsPkgName:Mod:$templateName")
    val v1TplId: Identifier = Identifier(templateDefsV1PkgId, tplQualifiedName)

    val clientContractId: ContractId = toContractId("client")
    val globalContractId: ContractId = toContractId("1")

    val clientContract: VersionedContractInstance = assertAsVersionedContract(
      ContractInstance(
        clientPkg.pkgName,
        Some(clientPkg.metadata.version),
        clientTplId,
        ValueRecord(
          Some(clientTplId),
          ImmArray(
            Some("p": Name) -> ValueParty(alice)
          ),
        ),
      )
    )

    val globalContractArg: ValueRecord = ValueRecord(
      Some(v1TplId),
      ImmArray(
        Some("p1": Name) -> ValueParty(alice),
        Some("p2": Name) -> ValueParty(bob),
      ).slowAppend(testCase.additionalCreateArgsValue(templateDefsV1PkgId)),
    )

    val globalContract: VersionedContractInstance = assertAsVersionedContract(
      ContractInstance(
        templateDefsV1Pkg.pkgName,
        Some(templateDefsV1Pkg.metadata.version),
        v1TplId,
        globalContractArg,
      )
    )

    val globalContractv1Key: ValueRecord = ValueRecord(
      None,
      ImmArray(
        Some("label": Name) -> ValueText("test-key"),
        Some("maintainers": Name) -> ValueList(FrontStack(ValueParty(alice))),
      ).slowAppend(testCase.additionalv1KeyArgsValue(templateDefsV1PkgId)),
    )

    val globalContractv2Key: ValueRecord = ValueRecord(
      None,
      ImmArray(
        Some("label": Name) -> ValueText("test-key"),
        Some("maintainers": Name) -> ValueList(FrontStack(ValueParty(alice))),
      ).slowAppend(testCase.additionalv2KeyArgsValue(templateDefsV2PkgId)),
    )

    val globalContractKeyWithMaintainers: GlobalKeyWithMaintainers =
      GlobalKeyWithMaintainers.assertBuild(
        v1TplId,
        globalContractv1Key,
        Set(alice),
        templateDefsPkgName,
      )

    def normalize(value: Value, typ: Ast.Type): Value = {
      Machine.fromPureSExpr(compiledPackages, SEImportValue(typ, value)).runPure() match {
        case Left(err) => throw new RuntimeException(s"Normalization failed: $err")
        case Right(sValue) => sValue.toNormalizedValue(languageVersion)
      }
    }

    val globalContractDisclosure: FatContractInstance = FatContractInstanceImpl(
      version = languageVersion,
      contractId = globalContractId,
      packageName = templateDefsPkgName,
      packageVersion = None,
      templateId = v1TplId,
      createArg = normalize(globalContractArg, Ast.TTyCon(v1TplId)),
      signatories = immutable.TreeSet(alice),
      stakeholders = immutable.TreeSet(alice),
      contractKeyWithMaintainers = Some(globalContractKeyWithMaintainers),
      createdAt = Time.Timestamp.Epoch,
      cantonData = Bytes.assertFromString("00"),
    )

    def toContractId(s: String): ContractId =
      ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

    def makeApiCommands(
        operation: Operation,
        catchBehavior: CatchBehavior,
        entryPoint: EntryPoint,
        contractOrigin: ContractOrigin,
    ): Option[ImmArray[ApiCommand]] = {

      val choiceArg = ValueRecord(
        None,
        testCase.choiceArgValue,
      )

      // We first rule out all non-sensical cases, and then proceed to create a command in the most generic way
      // possible. The good thing about this approach compared to a whitelist is that we won't accidentally forget
      // some cases. If we forget to blacklist some case, the test will simply fail.
      // Some of the patterns below are verbose and could be simplified with a pattern guard, but we favor this style
      // because it is compatible exhaustivness checker.
      (testCase, operation, catchBehavior, entryPoint, contractOrigin) match {
        // TODO(https://github.com/DACH-NY/canton/issues/23826): re-enable key upgrade/downgrade tests once
        //   SValue.toNormalizedValue drops trailing Nones. It currently doesn't, which means checkContractUpgradable
        //   will always fail on keys that differ, even if they normalize to the same value.
        case (
              ValidKeyUpgradeAdditionalField | ValidKeyDowngradeAdditionalField,
              _,
              _,
              _,
              _,
            ) =>
          None
        case (_, Fetch | FetchInterface | FetchByKey | LookupByKey, _, Command, _) =>
          None // There are no fetch* or lookupByKey commands
        case (_, Exercise | ExerciseInterface, _, Command, Local) =>
          None // Local contracts cannot be exercised by commands, except by key
        case (
              AdditionalFieldInChoiceArgRecordField | AdditionalConstructorInChoiceArgVariantField |
              AdditionalConstructorInChoiceArgEnumField | AdditionalChoiceArg |
              ValidDowngradeAdditionalFieldInChoiceArgRecordField |
              InvalidDowngradeAdditionalFieldInChoiceArgRecordField |
              ValidDowngradeAdditionalConstructorInChoiceArgVariantField |
              InvalidDowngradeAdditionalConstructorInChoiceArgVariantField |
              ValidDowngradeAdditionalConstructorInChoiceArgEnumField |
              InvalidDowngradeAdditionalConstructorInChoiceArgEnumField |
              ValidDowngradeAdditionalChoiceArg | InvalidDowngradeAdditionalChoiceArg,
              _,
              _,
              _,
              _,
            )
            if entryPoint == ChoiceBody || operation == FetchInterface || operation == ExerciseInterface =>
          None // *ChoiceArg* test cases only make sense for non-interface exercise commands
        case (
              ThrowingInterfaceChoiceControllers | ThrowingInterfaceChoiceObservers,
              Fetch | FetchInterface | FetchByKey | LookupByKey | Exercise | ExerciseByKey,
              _,
              _,
              _,
            ) =>
          None // ThrowingInterfaceChoice* test cases only makes sense for ExerciseInterface
        case (
              InvalidKeyDowngradeAdditionalField,
              ExerciseByKey | FetchByKey | LookupByKey,
              _,
              _,
              _,
            ) =>
          None // InvalidKeyDowngradeAdditionalField does not make sense for *ByKey operations
        case (ThrowingView, Fetch | FetchByKey | LookupByKey | Exercise | ExerciseByKey, _, _, _) =>
          None // ThrowingView only makes sense for *Interface operations
        case (_, Exercise, _, Command, Global | Disclosed) =>
          Some(
            ImmArray(
              ApiCommand.Exercise(
                tplRef, // we let package preference select v2
                globalContractId,
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              )
            )
          )
        case (_, ExerciseInterface, _, Command, Global | Disclosed) =>
          Some(
            ImmArray(
              ApiCommand.Exercise(
                ifaceId,
                globalContractId,
                ChoiceName.assertFromString("InterfaceChoice"),
                ValueUnit,
              )
            )
          )
        case (_, ExerciseByKey, _, Command, Global | Disclosed) =>
          Some(
            ImmArray(
              ApiCommand.ExerciseByKey(
                tplRef, // we let package preference select v2
                globalContractv2Key,
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              )
            )
          )
        case (_, ExerciseByKey, _, Command, Local) =>
          Some(
            ImmArray(
              ApiCommand.Create(
                v1TplId,
                globalContractArg,
              ),
              ApiCommand.ExerciseByKey(
                tplRef, // we let package preference select v2
                globalContractv2Key,
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              ),
            )
          )
        case (_, _, _, ChoiceBody, Global | Disclosed) =>
          Some(
            ImmArray(
              ApiCommand.Exercise(
                clientTplId,
                clientContractId,
                ChoiceName.assertFromString(
                  s"${operation.name}${catchBehavior.name}Global${templateName}"
                ),
                operation match {
                  case Fetch | FetchInterface | Exercise | ExerciseInterface =>
                    ValueContractId(globalContractId)
                  case FetchByKey | LookupByKey | ExerciseByKey =>
                    globalContractv2Key
                },
              )
            )
          )
        case (_, _, _, ChoiceBody, Local) =>
          Some(
            ImmArray(
              ApiCommand.Exercise(
                clientTplId,
                clientContractId,
                ChoiceName.assertFromString(
                  s"${operation.name}${catchBehavior.name}Local${templateName}"
                ),
                ValueUnit,
              )
            )
          )
      }
    }

    def newEngine() = new Engine(engineConfig)

    def execute(
        apiCommands: ImmArray[ApiCommand],
        contractOrigin: ContractOrigin,
    ): Either[Error, (SubmittedTransaction, Transaction.Metadata)] = {

      val participant = ParticipantId.assertFromString("participant")
      val submissionSeed = crypto.Hash.hashPrivateKey("command")
      val submitters = Set(alice)
      val readAs = Set.empty[Party]

      val disclosures = contractOrigin match {
        case Disclosed => ImmArray(globalContractDisclosure)
        case _ => ImmArray.empty
      }
      val lookupContractById = contractOrigin match {
        case Global => Map(clientContractId -> clientContract, globalContractId -> globalContract)
        case _ => Map(clientContractId -> clientContract)
      }
      val lookupContractByKey = contractOrigin match {
        case Global =>
          val keyMap = Map(globalContractKeyWithMaintainers.globalKey -> globalContractId)
          ((kwm: GlobalKeyWithMaintainers) => keyMap.get(kwm.globalKey)).unlift
        case _ => PartialFunction.empty
      }

      newEngine()
        .submit(
          packageMap = packageMap,
          packagePreference = Set(commonDefsPkgId, templateDefsV2PkgId, clientPkgId),
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(apiCommands, Time.Timestamp.Epoch, "test"),
          disclosures = disclosures,
          participantId = participant,
          submissionSeed = submissionSeed,
          prefetchKeys = Seq.empty,
        )
        .consume(
          pcs = lookupContractById,
          pkgs = lookupPackage,
          keys = lookupContractByKey,
        )
    }
  }
}
