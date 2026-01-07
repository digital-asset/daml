// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import cats.Monoid
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.archive.DamlLf._
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.testing.parser.{AstRewriter, ParserParameters}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.Assertion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn
import scala.concurrent.Future
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
  * It picks up its list of test scenarios from [[cases]], which is an instance
  * of [[UpgradesMatrixCases]] class - consult the documentation for the class for
  * more about how the test cases are generated.
  *
  * It can be parameterized with (n, k) to split the tests into n pieces, and
  * only run the ith piece. We can use this to split tests over multiple suites
  * that get run in parallel, i.e.
  *
  * class MyRunner0 extends UpgradesMatrix[...](..., nk = Some(4, 0)) { ... }
  * class MyRunner1 extends UpgradesMatrix[...](..., nk = Some(4, 1)) { ... }
  * class MyRunner2 extends UpgradesMatrix[...](..., nk = Some(4, 2)) { ... }
  * class MyRunner3 extends UpgradesMatrix[...](..., nk = Some(4, 3)) { ... }
  *
  * will run a quarter of the tests with MyRunner0, another quarter over
  * MyRunner1, and so on, all in parallel.
  *
  * This gives small improvements for unit tests where the processor is already
  * pretty well utilized, but gives big improvements for integration tests with
  * multiple Canton runners.
  */
abstract class UpgradesMatrix[Err, Res](
    val cases: UpgradesMatrixCases,
    nk: Option[(Int, Int)] = None,
) extends AsyncFreeSpec
    with Matchers {
  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  def execute(
      setupData: UpgradesMatrixCases.SetupData,
      testHelper: cases.TestHelper,
      apiCommands: ImmArray[ApiCommand],
      contractOrigin: UpgradesMatrixCases.ContractOrigin,
      creationPackageStatus: UpgradesMatrixCases.CreationPackageStatus,
  ): Future[Either[Err, Res]]

  def setup(testHelper: cases.TestHelper): Future[UpgradesMatrixCases.SetupData]

  def assertResultMatchesExpectedOutcome(
      result: Either[Err, Res],
      expectedOutcome: UpgradesMatrixCases.ExpectedOutcome,
  ): Assertion

  // Use this to run different sets of tests for different suites
  var testIdx: Int = 0

  // This is the main loop of the test: for every combination of test case, operation, catch behavior, entry point, and
  // contract origin, we generate an API command, execute it, and check that the result matches the expected outcome.
  for (testCase <- cases.testCases) {
    val testHelper = new cases.TestHelper(testCase)
    for (operation <- cases.operations) {
      for (catchBehavior <- cases.catchBehaviors) {
        for (entryPoint <- cases.entryPoints) {
          for (contractOrigin <- cases.contractOrigins) {
            for (creationPackageStatus <- cases.creationPackageStatuses) {
              testHelper
                .makeApiCommands(
                  operation,
                  catchBehavior,
                  entryPoint,
                  contractOrigin,
                  creationPackageStatus,
                )
                .foreach { getApiCommands =>
                  val shouldShow =
                    // If n, k is specified, then only run tests that belong to the kth group
                    nk match {
                      case Some((n, k)) => testIdx % n == k
                      case None => true
                    }
                  if (shouldShow) {
                    val title =
                      List(
                        testCase.templateName,
                        operation.name,
                        catchBehavior.name,
                        entryPoint.name,
                        contractOrigin.name,
                        creationPackageStatus.toString,
                        testCase.expectedOutcome.description,
                        testIdx.toString,
                      ).mkString("/")
                    title in {
                      for {
                        setupData <- setup(testHelper)
                        apiCommands = getApiCommands(setupData)
                        outcome <- execute(
                          setupData,
                          testHelper,
                          apiCommands,
                          contractOrigin,
                          creationPackageStatus,
                        )
                      } yield assertResultMatchesExpectedOutcome(
                        outcome,
                        testCase.expectedOutcome,
                      )
                    }
                  }
                  testIdx += 1
                }
            }
          }
        }
      }
    }
  }
}

// Instances of UpgradesMatrixCases which provide cases built with LF 2.dev or the
// highest stable 2.x version, respectively
object UpgradesMatrixCasesV2Dev extends UpgradesMatrixCases(LanguageVersion.v2_dev)
object UpgradesMatrixCasesV2MaxStable
    extends UpgradesMatrixCases(LanguageVersion.latestStableLfVersion)

/** Pairs of v1/v2 templates are called [[TestCase]]s and are listed in [[testCases]]. A test case is defined by
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
  * In order to test scenarios where the operation is triggered by a choice body, we need "clients" contracts whose only
  * role is to exercise/fetch/lookup the v1 contract. The templates for these client contracts are defined in
  * [[clientLocalPkg]] and [[clientGlobalPkg]]. They are split into two templates: one whose choices creates local
  * v1 contracts, and one whose choices act on global contracts by fetching them. The former statically refers to v1 templates while
  * the latter doesn't, allowing us to test cases where the creation package of the contract being upgraded is not
  * vetted.
  * They define a large number of choices: one per combination of operation, catch behavior, entry point and test case.
  * These choices are defined in [[TestCase.clientChoicesLocal]] and [[TestCase.clientChoicesGlobal()]]. These methods
  * are pretty boilerplate-y and constitute the bulk of this test suite.
  *
  * Finally, some definitions need to be shared between v1 and v2 templates: key and interface definitions, gobal
  * parties. These are defined in [[commonDefsPkg]].
  */
class UpgradesMatrixCases(
    val langVersion: LanguageVersion
) {
  import UpgradesMatrixCases._

  private[this] def parserParameters(implicit
      pkgId: PackageId
  ): ParserParameters[this.type] =
    ParserParameters(
      pkgId,
      languageVersion = langVersion,
    )

  def ifKeys[A](ifTrue: => A, ifFalse: => A): A =
    if (LanguageVersion.featureContractKeys.enabledIn(langVersion)) ifTrue else ifFalse
  def whenKeysOtherwiseNone[A](a: => A): Option[A] = ifKeys(Some(a), None)
  def whenKeysOtherwiseEmpty[A](a: => A)(implicit m: Monoid[A]) = ifKeys(a, m.empty)

  val serializationVersion = SerializationVersion.assign(langVersion)

  def encodeDalfArchive(
      pkgId: PackageId,
      pkgSrc: String,
      lfVersion: LanguageVersion = langVersion,
  ): (String, Archive, Ast.Package, PackageId) = {
    val pkg = p"${pkgSrc}" (parserParameters(pkgId))
    val archive = Encode.encodeArchive(pkgId -> pkg, lfVersion)
    val computedPkgId = PackageId.assertFromString(archive.getHash)
    val dalfName = s"${pkg.metadata.nameDashVersion}-$computedPkgId.dalf"
    val updatedPkg = (new AstRewriter(packageIdRule = { case `pkgId` => computedPkgId })).apply(pkg)
    (dalfName, archive, updatedPkg, computedPkgId)
  }

  // implicit conversions from strings to names and qualified names
  implicit def toName(s: String): Name = Name.assertFromString(s)
  implicit def toQualifiedName(s: String): QualifiedName = QualifiedName.assertFromString(s)

  val stablePackages =
    com.digitalasset.daml.lf.stablepackages.StablePackages.stablePackages

  val tuple2TyCon: String = {
    import stablePackages.Tuple2
    s"'${Tuple2.packageId}':${Tuple2.qualifiedName}"
  }

  // A package that defines an interface, a key type, an exception, and a party to be used by
  // the packages defined below.
  val (commonDefsDalfName, commonDefsDalf, commonDefsPkg, commonDefsPkgId) =
    encodeDalfArchive(
      PackageId.assertFromString("-common-defs-id-"),
      """metadata ( '-common-defs-' : '1.0.0' )
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
            }
        """,
    )

  /** Represents an LF value specified both as some string we can splice into the textual representation of LF, and as a
    * scala value we can splice into an [[ApiCommand]].
    */
  case class TestCaseValue[A](inChoiceBodyLfCode: String, inApiCommand: A)

  /** An abstract class whose [[v1TemplateDefinition]], [[v2TemplateDefinition]] and [[clientChoicesLocal]] methods generate
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
    def v1InterfaceInstance: String =
      s"""
         |    implements '$commonDefsPkgId':Mod:Iface {
         |      view = $v1View;
         |      method interfaceChoiceControllers = $v1InterfaceChoiceControllers;
         |      method interfaceChoiceObservers = $v1InterfaceChoiceObservers;
         |    };"""

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
    def v2InterfaceInstance: String =
      s"""
         |    implements '$commonDefsPkgId':Mod:Iface {
         |      view = $v2View;
         |      method interfaceChoiceControllers = $v2InterfaceChoiceControllers;
         |      method interfaceChoiceObservers = $v2InterfaceChoiceObservers;
         |    };"""

    // Used for creating contracts in choice bodies
    def additionalCreateArgsLf(v1PkgId: PackageId): String = ""
    // Used for creating contracts in commands
    def additionalCreateArgsValue(@nowarn v1PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray.empty

    // Used for creating disclosures of v1 contracts and the "lookup contract by key" map passed to the engine.
    def additionalv1KeyArgsValue(
        @nowarn v1PkgId: PackageId,
        @nowarn setupData: SetupData,
    ): ImmArray[(Option[Name], Value)] =
      ImmArray.empty
    // Used for creating *ByKey commands
    def additionalv2KeyArgsValue(
        @nowarn v2PkgId: PackageId,
        @nowarn setupData: SetupData,
    ): ImmArray[(Option[Name], Value)] =
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
        interfaceInstance: String,
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
         |    $interfaceInstance
         |
         |    ${whenKeysOtherwiseEmpty(s"key @Mod:${templateName}Key ($key) ($maintainers);")}
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
      v1InterfaceInstance,
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
      v2InterfaceInstance,
    )

    def clientChoicesGlobal(
        v2PkgId: PackageId
    ): String = {
      val clientTplQualifiedName = "Mod:Client"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      val ifaceQualifiedName = s"'$commonDefsPkgId':Mod:Iface"
      val v2KeyTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}Key"
      val v2ChoiceArgTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}ChoiceArgType"

      val choiceArgExpr = s"($v2ChoiceArgTypeQualifiedName {})"

      val nonByKeyChoices = s"""
         |  choice @nonConsuming ExerciseNoCatchGlobal${templateName} (self) (cid: ContractId $v2TplQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise
         |         @$v2TplQualifiedName
         |         TemplateChoice
         |         cid
         |         $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseAttemptCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $v2TplQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseInterfaceNoCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $ifaceQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise_interface
         |         @$ifaceQualifiedName
         |         InterfaceChoice
         |         cid
         |         ();
         |
         |  choice @nonConsuming ExerciseInterfaceAttemptCatchGlobal${templateName}
         |        (self)
         |        (cid: ContractId $ifaceQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseInterfaceNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchNoCatchGlobal${templateName} (self) (cid: ContractId $v2TplQualifiedName)
         |        : $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to fetch_template
         |         @$v2TplQualifiedName
         |         cid;
         |
         |  choice @nonConsuming FetchAttemptCatchGlobal${templateName} (self) (cid: ContractId $v2TplQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchInterfaceNoCatchGlobal${templateName} (self) (cid: ContractId $ifaceQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind iface: $ifaceQualifiedName <- fetch_interface
         |         @$ifaceQualifiedName
         |         cid
         |       in upure @Text "no exception was caught";
         |
         |  choice @nonConsuming FetchInterfaceAttemptCatchGlobal${templateName} (self) (cid: ContractId $ifaceQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <-
         |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchGlobal${templateName} self cid
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |""".stripMargin

      val byKeyChoices = s"""
         |  choice @nonConsuming ExerciseByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to exercise_by_key
         |         @$v2TplQualifiedName
         |         TemplateChoice
         |         key
         |         $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind pair:$tuple2TyCon (ContractId $v2TplQualifiedName) $v2TplQualifiedName <-
         |          fetch_by_key
         |            @$v2TplQualifiedName
         |            key
         |       in upure @$v2TplQualifiedName ($tuple2TyCon @(ContractId $v2TplQualifiedName) @$v2TplQualifiedName {_2} pair);
         |
         |  choice @nonConsuming FetchByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming LookupByKeyNoCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Option (ContractId $v2TplQualifiedName)
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to lookup_by_key
         |         @$v2TplQualifiedName
         |         key;
         |
         |  choice @nonConsuming LookupByKeyAttemptCatchGlobal${templateName} (self) (key: $v2KeyTypeQualifiedName)
         |        : Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Option (ContractId $v2TplQualifiedName) <-
         |             exercise @$clientTplQualifiedName LookupByKeyNoCatchGlobal${templateName} self key
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |""".stripMargin

      nonByKeyChoices + whenKeysOtherwiseEmpty(byKeyChoices)
    }

    def clientChoicesLocal(
        v1PkgId: PackageId,
        v2PkgId: PackageId,
    ): String = {
      val clientTplQualifiedName = "Mod:Client"
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      val ifaceQualifiedName = s"'$commonDefsPkgId':Mod:Iface"
      val v2KeyTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}Key"
      val v2ChoiceArgTypeQualifiedName = s"'$v2PkgId':Mod:${templateName}ChoiceArgType"

      val createV1ContractExpr =
        s""" create
           |    @$v1TplQualifiedName
           |    ($v1TplQualifiedName
           |        { p1 = Mod:Client {alice} this
           |        , p2 = Mod:Client {bob} this
           |        ${additionalCreateArgsLf(v1PkgId)}
           |        })
           |""".stripMargin

      val v2KeyExpr =
        s""" ($v2KeyTypeQualifiedName
           |     { label = "test-key"
           |     , maintainers = (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |     ${additionalv2KeyArgsLf(v2PkgId)}
           |     })""".stripMargin

      val choiceArgExpr = s"($v2ChoiceArgTypeQualifiedName {})"

      val nonByKeyChoices = s"""
         |  choice @nonConsuming ExerciseNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in exercise
         |            @$v2TplQualifiedName
         |            TemplateChoice
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
         |            $choiceArgExpr;
         |
         |  choice @nonConsuming ExerciseAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming ExerciseInterfaceNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in exercise_interface
         |            @$ifaceQualifiedName
         |            InterfaceChoice
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |            ();
         |
         |  choice @nonConsuming ExerciseInterfaceAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseInterfaceNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchNoCatchLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
         |       in fetch_template
         |            @$v2TplQualifiedName
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid);
         |
         |  choice @nonConsuming FetchAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:$v2TplQualifiedName <-
         |             exercise @$clientTplQualifiedName FetchNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |
         |  choice @nonConsuming FetchInterfaceNoCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to ubind
         |         cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr;
         |         iface: $ifaceQualifiedName <- fetch_interface
         |            @$ifaceQualifiedName
         |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
         |         in upure @Text "no exception was caught";
         |
         |  choice @nonConsuming FetchInterfaceAttemptCatchLocal${templateName} (self) (u: Unit): Text
         |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
         |    , observers (Nil @Party)
         |    to try @Text
         |         ubind __:Text <-
         |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchLocal${templateName} self ()
         |         in upure @Text "no exception was caught"
         |       catch
         |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
         |""".stripMargin

      val byKeyChoices =
        s"""
           |  choice @nonConsuming ExerciseByKeyNoCatchLocal${templateName} (self) (u: Unit): Text
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
           |       in exercise_by_key
           |            @$v2TplQualifiedName
           |            TemplateChoice
           |            $v2KeyExpr
           |            $choiceArgExpr;
           |
           |  choice @nonConsuming ExerciseByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to try @Text
           |         ubind __:Text <- exercise @$clientTplQualifiedName ExerciseByKeyNoCatchLocal${templateName} self ()
           |         in upure @Text "no exception was caught"
           |       catch
           |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
           |
           |  choice @nonConsuming FetchByKeyNoCatchLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
           |       in ubind pair:$tuple2TyCon (ContractId $v2TplQualifiedName) $v2TplQualifiedName <-
           |              fetch_by_key
           |                @$v2TplQualifiedName
           |                $v2KeyExpr
           |          in upure @$v2TplQualifiedName ($tuple2TyCon @(ContractId $v2TplQualifiedName) @$v2TplQualifiedName {_2} pair);
           |
           |  choice @nonConsuming FetchByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to try @Text
           |         ubind __:$v2TplQualifiedName <-
           |             exercise @$clientTplQualifiedName FetchByKeyNoCatchLocal${templateName} self ()
           |         in upure @Text "no exception was caught"
           |       catch
           |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
           |
           |  choice @nonConsuming LookupByKeyNoCatchLocal${templateName} (self) (u: Unit): Option (ContractId $v2TplQualifiedName)
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to ubind cid: ContractId $v1TplQualifiedName <- $createV1ContractExpr
           |       in lookup_by_key
           |            @$v2TplQualifiedName
           |            $v2KeyExpr;
           |
           |  choice @nonConsuming LookupByKeyAttemptCatchLocal${templateName} (self) (u: Unit): Text
           |    , controllers (Cons @Party [Mod:Client {alice} this] (Nil @Party))
           |    , observers (Nil @Party)
           |    to try @Text
           |         ubind __:Option (ContractId $v2TplQualifiedName) <-
           |             exercise @$clientTplQualifiedName LookupByKeyNoCatchLocal${templateName} self ()
           |         in upure @Text "no exception was caught"
           |       catch
           |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
           |""".stripMargin

      nonByKeyChoices + whenKeysOtherwiseEmpty(byKeyChoices)
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

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: added an interface instance in v2 when it doesn't yet exist in v1, make sure that it gets picked up
  case object AddingInterfaceInstance extends TestCase("AddingInterfaceInstance", ExpectSuccess) {
    override def v1InterfaceInstance = ""
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
    override def additionalv1KeyArgsValue(pkgId: PackageId, setupData: SetupData) =
      ImmArray(
        None /* maintainers2 */ -> ValueList(
          FrontStack(ValueParty(setupData.bob))
        )
      )
    // Used for creating *ByKey commands
    override def additionalv2KeyArgsValue(pkgId: PackageId, setupData: SetupData) =
      additionalv1KeyArgsValue(pkgId, setupData)
    // Used for looking up contracts by key in choice bodies
    override def additionalv2KeyArgsLf(v2PkgId: PackageId) =
      s", maintainers2 = (Cons @Party [Mod:Client {bob} this] (Nil @Party))"

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
        None /* r */ -> ValueRecord(
          None,
          ImmArray(
            None /* n */ -> ValueInt64(0),
            None /* extra */ -> ValueOptional(Some(ValueUnit)),
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
        None /* r */ -> ValueRecord(
          None,
          ImmArray(
            None /* n */ -> ValueInt64(0),
            None /* extra */ -> ValueOptional(None),
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
        None /* r */ -> ValueRecord(
          None,
          ImmArray(
            None /* n */ -> ValueInt64(0),
            None /* extra */ -> ValueOptional(Some(ValueUnit)),
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
      ImmArray(
        None /* r */ -> ValueRecord(
          None /* Mod:$recordName */,
          ImmArray(
            None /* n */ -> ValueInt64(0)
          ),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: differently-named field in record used as template parameter, upgrade fails
  case object DifferentlyNamedFieldInRecordArg
      extends TestCase("DifferentlyNamedFieldInRecordArg", ExpectAuthenticationError) {

    val recordName = s"${templateName}Record"

    override def v1AdditionalDefinitions: String =
      s"record @serializable $recordName = { n : Int64 };"
    override def v2AdditionalDefinitions: String =
      s"record @serializable $recordName = { nRenamed : Int64 };"

    override def v1AdditionalFields = s", r: Mod:$recordName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedRecordName = s"'$v1PkgId':Mod:$recordName"
      s", r = $qualifiedRecordName { n = 0 }"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        None /* r */ -> ValueRecord(
          None /* Mod:$recordName */,
          ImmArray(
            None /* n */ -> ValueInt64(0)
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
      ImmArray(
        None /* v */ -> ValueVariant(
          None,
          "Ctor1",
          ValueInt64(0),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: differently-ranked variant constructor used as template parameter, upgrade fails
  case object DifferentlyRankedConstructorInVariantArg
      extends TestCase("DifferentlyRankedConstructorInVariantArg", ExpectAuthenticationError) {

    val variantName = s"${templateName}Variant"

    override def v1AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor1: Int64 | Ctor2: Unit;"
    override def v2AdditionalDefinitions: String =
      s"variant @serializable $variantName = Ctor2: Unit | Ctor1: Int64;"

    override def v1AdditionalFields = s", v: Mod:$variantName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedVariantName = s"'$v1PkgId':Mod:$variantName"
      s", v = $qualifiedVariantName:Ctor1 0"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        None /* v */ -> ValueVariant(
          None,
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
      ImmArray(
        None /* e */ -> ValueEnum(
          None,
          "Ctor1",
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: differently-ranked constructor in enum used as template parameter, upgrade fails
  case object DifferentlyRankedConstructorInEnumArg
      extends TestCase("DifferentlyRankedConstructorInEnumArg", ExpectAuthenticationError) {

    val enumName = s"${templateName}Enum"

    override def v1AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor1 | Ctor2;"
    override def v2AdditionalDefinitions: String =
      s"enum @serializable $enumName = Ctor2 | Ctor1;"

    override def v1AdditionalFields = s", e: Mod:$enumName"
    override def v2AdditionalFields = v1AdditionalFields

    override def additionalCreateArgsLf(v1PkgId: PackageId): String = {
      val qualifiedEnumName = s"'$v1PkgId':Mod:$enumName"
      s", e = $qualifiedEnumName:Ctor1"
    }

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        None /* e */ -> ValueEnum(
          None,
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

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: differently-named template parameter, upgrade fails
  case object DifferentlyNamedTemplateArg
      extends TestCase("DifferentlyNamedTemplateArg", ExpectAuthenticationError) {
    override def v1AdditionalFields = ", extra: Unit"
    override def v2AdditionalFields = ", extraRenamed: Unit"

    override def additionalCreateArgsLf(v1PkgId: PackageId): String =
      s", extra = ()"

    override def additionalCreateArgsValue(v1PkgId: PackageId): ImmArray[(Option[Name], Value)] =
      ImmArray(None /* extra */ -> ValueUnit)
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
        None /* extra */ -> ValueOptional(None)
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional template parameter, None arg, downgrade succeeds
  case object InvalidDowngradeAdditionalTemplateArg
      extends TestCase("InvalidDowngradeAdditionalTemplateArg", ExpectRuntimeTypeMismatchError) {
    override def v1AdditionalFields = ", extra: Option Unit"
    override def v2AdditionalFields = ""

    override def additionalCreateArgsLf(v1PkgId: PackageId): String =
      s", extra = Some @Unit ()"

    override def additionalCreateArgsValue(v1PkgId: PackageId) = {
      ImmArray(
        None /* extra */ -> ValueOptional(Some(ValueUnit))
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
      ImmArray(
        None /* r */ -> ValueRecord(
          None /* Mod:$recordName */,
          ImmArray(
            None /* n */ -> ValueInt64(0),
            None /* extra */ -> ValueOptional(None),
          ),
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional optional field in record used as template parameter, Some value, downgrade fails
  case object InvalidDowngradeAdditionalFieldInRecordArg
      extends TestCase(
        "InvalidDowngradeAdditionalFieldInRecordArg",
        ExpectRuntimeTypeMismatchError,
      ) {

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
      ImmArray(
        None /* r */ -> ValueRecord(
          None /* Mod:$recordName */,
          ImmArray(
            None /* n */ -> ValueInt64(0),
            None /* extra */ -> ValueOptional(Some(ValueUnit)),
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
      ImmArray(
        None /* v */ -> ValueVariant(
          None,
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
        ExpectRuntimeTypeMismatchError,
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
      ImmArray(
        None /* v */ -> ValueVariant(
          None,
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
      ImmArray(
        None /* e */ -> ValueEnum(
          None,
          "Ctor1",
        )
      )
    }
  }

  // TEST_EVIDENCE: Integrity: Smart Contract Upgrade: additional constructor in enum used as template parameter, used constructor, downgrade fails
  case object InvalidDowngradeAdditionalConstructorInEnumArg
      extends TestCase(
        "InvalidDowngradeAdditionalConstructorInEnumArg",
        ExpectRuntimeTypeMismatchError,
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
      ImmArray(
        None /* e */ -> ValueEnum(
          None,
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
    override def additionalv2KeyArgsValue(v2PkgId: PackageId, setupData: SetupData) =
      ImmArray(
        None /* extra */ -> ValueOptional(None)
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
    override def additionalv2KeyArgsValue(v2PkgId: PackageId, setupData: SetupData) =
      ImmArray(
        None /* extra */ -> ValueOptional(None)
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
    override def additionalv1KeyArgsValue(
        v1PkgId: PackageId,
        setupData: SetupData,
    ): ImmArray[(Option[Name], Value)] =
      ImmArray(
        None /* extra */ -> ValueOptional(Some(ValueUnit))
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
    // template arg
    AdditionalFieldInRecordArg,
    DifferentlyNamedFieldInRecordArg,
    AdditionalConstructorInVariantArg,
    DifferentlyRankedConstructorInVariantArg,
    AdditionalConstructorInEnumArg,
    DifferentlyRankedConstructorInEnumArg,
    AdditionalTemplateArg,
    AddingInterfaceInstance,
    DifferentlyNamedTemplateArg,
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
  ) ++ whenKeysOtherwiseEmpty(
    List(
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
    )
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
  val (templateDefsV1DalfName, templateDefsV1Dalf, templateDefsV1Pkg, templateDefsV1PkgId) =
    encodeDalfArchive(
      PackageId.assertFromString("-template-defs-v1-id-"),
      s"""metadata ( '$templateDefsPkgName' : '1.0.0' )
            module Mod {
              ${testCases.map(_.v1TemplateDefinition).mkString("\n")}
            }
        """,
    )

  /** Version 2 of the package above. It upgrades the previously defined templates such that:
    *   - the precondition in the Precondition template is changed to throw an exception
    *   - the signatories in the Signatories template is changed to throw an exception
    *   - etc.
    */
  val (templateDefsV2DalfName, templateDefsV2Dalf, templateDefsV2Pkg, templateDefsV2PkgId) =
    encodeDalfArchive(
      PackageId.assertFromString("-template-defs-v2-id-"),
      s"""metadata ( '$templateDefsPkgName' : '2.0.0' )
            module Mod {
              ${testCases.map(_.v2TemplateDefinition).mkString("\n")}
            }
        """,
    )

  val (clientLocalDalfName, clientLocalDalf, clientLocalPkg, clientLocalPkgId) =
    encodeDalfArchive(
      PackageId.assertFromString("-client-local-id-"), {
        val choices = commandAndChoiceTestCases
          .map(_.clientChoicesLocal(templateDefsV1PkgId, templateDefsV2PkgId))
        s"""metadata ( '-client-local-' : '1.0.0' )
                module Mod {
                  record @serializable Client = { alice: Party, bob: Party };
                  template (this: Client) = {
                    precondition True;
                    signatories Cons @Party [Mod:Client {alice} this] (Nil @Party);
                    observers Nil @Party;

                    ${choices.mkString("\n")}
                  };
                }
            """
      },
    )

  val (clientGlobalDalfName, clientGlobalDalf, clientGlobalPkg, clientGlobalPkgId) =
    encodeDalfArchive(
      PackageId.assertFromString("-client-global-id-"), {
        val choices = commandAndChoiceTestCases
          .map(_.clientChoicesGlobal(templateDefsV2PkgId))
        s"""metadata ( '-client-global-' : '1.0.0' )
                module Mod {
                  record @serializable Client = { alice: Party, bob: Party };
                  template (this: Client) = {
                    precondition True;
                    signatories Cons @Party [Mod:Client {alice} this] (Nil @Party);
                    observers Nil @Party;

                    ${choices.mkString("\n")}
                  };
                }
            """
      },
    )

  /** The package used for the creation (local or global) v1 contracts and packages that depend on them. */
  val allCreationPackages: Map[PackageId, Ast.Package] =
    Map(templateDefsV1PkgId -> templateDefsV1Pkg, clientLocalPkgId -> clientLocalPkg)

  /** The complement of [[allCreationPackages]]. */
  val allNonCreationPackages: Map[PackageId, Ast.Package] = Map(
    stablePackages.Tuple2.packageId -> stablePackages.packagesMap(stablePackages.Tuple2.packageId),
    commonDefsPkgId -> commonDefsPkg,
    templateDefsV2PkgId -> templateDefsV2Pkg,
    clientGlobalPkgId -> clientGlobalPkg,
  )

  val allPackages: Map[PackageId, Ast.Package] = allNonCreationPackages ++ allCreationPackages

  val packageMap: Map[PackageId, (PackageName, PackageVersion)] =
    allPackages.view.mapValues(pkg => (pkg.pkgName, pkg.metadata.version)).toMap

  val engineConfig: EngineConfig =
    EngineConfig(
      allowedLanguageVersions = language.LanguageVersion.allUpToVersion(langVersion)
    )

  val contractIdVersion: ContractIdVersion = ContractIdVersion.V1

  val compiledPackages: PureCompiledPackages =
    PureCompiledPackages.assertBuild(allPackages, engineConfig.getCompilerConfig)

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
      ExerciseInterface,
      Fetch,
      FetchInterface,
    ) ++ whenKeysOtherwiseEmpty(
      List(
        ExerciseByKey,
        FetchByKey,
        LookupByKey,
      )
    )

  sealed abstract class CatchBehavior(val name: String)
  case object AttemptCatch extends CatchBehavior("AttemptCatch")
  case object NoCatch extends CatchBehavior("NoCatch")

  val catchBehaviors: List[CatchBehavior] = List(AttemptCatch, NoCatch)

  sealed abstract class EntryPoint(val name: String)
  case object Command extends EntryPoint("Command")
  case object ChoiceBody extends EntryPoint("ChoiceBody")

  val entryPoints: List[EntryPoint] = List(Command, ChoiceBody)

  val contractOrigins: List[ContractOrigin] = List(Global, Disclosed, Local)

  val creationPackageStatuses: List[CreationPackageStatus] =
    List(CreationPackageVetted, CreationPackageUnvetted)

  /** A class that defines all the "global" variables shared by tests for a given template name: the template ID of the
    * v1 template, the template ID of the v2 template, the ID of the v1 contract, etc. It exposes two methods:
    * - [[makeApiCommands]], which generates an API command for a given operation, catch behavior, entry point,
    *   and contract origin.
    * - [[execute]], which executes a command against a fresh engine seeded with the v1 contract.
    */
  class TestHelper(testCase: TestCase) {
    val templateName = testCase.templateName

    val clientLocalTplId: Identifier = Identifier(clientLocalPkgId, "Mod:Client")
    val clientGlobalTplId: Identifier = Identifier(clientGlobalPkgId, "Mod:Client")
    val ifaceId: Identifier = Identifier(commonDefsPkgId, "Mod:Iface")
    val tplQualifiedName: QualifiedName = s"Mod:$templateName"
    val tplRef: TypeConRef = TypeConRef.assertFromString(s"#$templateDefsPkgName:Mod:$templateName")
    val v1TplId: Identifier = Identifier(templateDefsV1PkgId, tplQualifiedName)

    def clientContractArg(alice: Party, bob: Party): ValueRecord = ValueRecord(
      None /* clientTplId */,
      ImmArray(
        None /* alice */ -> ValueParty(alice),
        None /* bob */ -> ValueParty(bob),
      ),
    )

    def globalContractArg(alice: Party, bob: Party): ValueRecord = ValueRecord(
      None /* v1TplId */,
      ImmArray(
        None /* p1 */ -> ValueParty(alice),
        None /* p2 */ -> ValueParty(bob),
      ).slowAppend(testCase.additionalCreateArgsValue(templateDefsV1PkgId)),
    )

    def globalContractv1Key(setupData: SetupData): ValueRecord = ValueRecord(
      None,
      ImmArray(
        None /* label */ -> ValueText("test-key"),
        None /* maintainers */ -> ValueList(FrontStack(ValueParty(setupData.alice))),
      ).slowAppend(testCase.additionalv1KeyArgsValue(templateDefsV1PkgId, setupData)),
    )

    def globalContractv2Key(setupData: SetupData): ValueRecord = ValueRecord(
      None,
      ImmArray(
        None /* label */ -> ValueText("test-key"),
        None /* maintainers */ -> ValueList(FrontStack(ValueParty(setupData.alice))),
      ).slowAppend(testCase.additionalv2KeyArgsValue(templateDefsV2PkgId, setupData)),
    )

    def globalContractKeyWithMaintainers(setupData: SetupData): Option[GlobalKeyWithMaintainers] =
      whenKeysOtherwiseNone(
        GlobalKeyWithMaintainers.assertBuild(
          v1TplId,
          globalContractv1Key(setupData),
          Set(setupData.alice),
          templateDefsPkgName,
        )
      )

    def makeApiCommands(
        operation: Operation,
        catchBehavior: CatchBehavior,
        entryPoint: EntryPoint,
        contractOrigin: ContractOrigin,
        creationPackageStatus: CreationPackageStatus,
    ): Option[SetupData => ImmArray[ApiCommand]] = {

      val choiceArg = ValueRecord(
        None,
        testCase.choiceArgValue,
      )

      // We first rule out all non-sensical cases, and then proceed to create a command in the most generic way
      // possible. The good thing about this approach compared to a whitelist is that we won't accidentally forget
      // some cases. If we forget to blacklist some case, the test will simply fail.
      // Some of the patterns below are verbose and could be simplified with a pattern guard, but we favor this style
      // because it is compatible exhaustivness checker.
      (
        testCase,
        operation,
        catchBehavior,
        entryPoint,
        contractOrigin,
        creationPackageStatus,
      ) match {
        case (_, _, _, _, Local, CreationPackageUnvetted) =>
          None // local contracts cannot be created from unvetted packages
        case (_, Fetch | FetchInterface | FetchByKey | LookupByKey, _, Command, _, _) =>
          None // There are no fetch* or lookupByKey commands
        case (_, Exercise | ExerciseInterface, _, Command, Local, _) =>
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
              _,
            ) =>
          None // ThrowingInterfaceChoice* test cases only makes sense for ExerciseInterface
        case (
              InvalidKeyDowngradeAdditionalField,
              ExerciseByKey | FetchByKey | LookupByKey,
              _,
              _,
              _,
              _,
            ) =>
          None // InvalidKeyDowngradeAdditionalField does not make sense for *ByKey operations
        case (
              ThrowingView,
              Fetch | FetchByKey | LookupByKey | Exercise | ExerciseByKey,
              _,
              _,
              _,
              _,
            ) =>
          None // ThrowingView only makes sense for *Interface operations
        case (_, Exercise, _, Command, Global | Disclosed, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.Exercise(
                tplRef, // we let package preference select v2
                setupData.globalContractId,
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              )
            )
          )
        case (_, ExerciseInterface, _, Command, Global | Disclosed, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.Exercise(
                ifaceId.toRef,
                setupData.globalContractId,
                ChoiceName.assertFromString("InterfaceChoice"),
                ValueUnit,
              )
            )
          )
        case (_, ExerciseByKey, _, Command, Global | Disclosed, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.ExerciseByKey(
                tplRef, // we let package preference select v2
                globalContractv2Key(setupData),
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              )
            )
          )
        case (_, ExerciseByKey, _, Command, Local, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.Create(
                v1TplId.toRef,
                globalContractArg(setupData.alice, setupData.bob),
              ),
              ApiCommand.ExerciseByKey(
                tplRef, // we let package preference select v2
                globalContractv2Key(setupData),
                ChoiceName.assertFromString("TemplateChoice"),
                choiceArg,
              ),
            )
          )
        case (_, _, _, ChoiceBody, Global | Disclosed, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.Exercise(
                clientGlobalTplId.toRef,
                setupData.clientGlobalContractId,
                ChoiceName.assertFromString(
                  s"${operation.name}${catchBehavior.name}Global${templateName}"
                ),
                operation match {
                  case Fetch | FetchInterface | Exercise | ExerciseInterface =>
                    ValueContractId(setupData.globalContractId)
                  case FetchByKey | LookupByKey | ExerciseByKey =>
                    globalContractv2Key(setupData)
                },
              )
            )
          )
        case (_, _, _, ChoiceBody, Local, _) =>
          Some(setupData =>
            ImmArray(
              ApiCommand.Exercise(
                clientLocalTplId.toRef,
                setupData.clientLocalContractId,
                ChoiceName.assertFromString(
                  s"${operation.name}${catchBehavior.name}Local${templateName}"
                ),
                ValueUnit,
              )
            )
          )
      }
    }
  }
}

object UpgradesMatrixCases {
  sealed abstract class ExpectedOutcome(val description: String)
  case object ExpectSuccess extends ExpectedOutcome("should succeed")
  case object ExpectPreconditionViolated
      extends ExpectedOutcome("should fail with a precondition violated error")
  case object ExpectRuntimeTypeMismatchError
      extends ExpectedOutcome("should fail with a runtime type mismatch error")
  case object ExpectUpgradeError extends ExpectedOutcome("should fail with an upgrade error")
  case object ExpectAuthenticationError
      extends ExpectedOutcome("should fail with an authentication error")
  case object ExpectPreprocessingError
      extends ExpectedOutcome("should fail with a preprocessing error")
  case object ExpectUnhandledException
      extends ExpectedOutcome("should fail with an unhandled exception")
  case object ExpectInternalInterpretationError
      extends ExpectedOutcome("should fail with an internal interpretation error")

  case class SetupData(
      alice: Party,
      bob: Party,
      clientLocalContractId: ContractId,
      clientGlobalContractId: ContractId,
      globalContractId: ContractId,
  )

  sealed abstract class ContractOrigin(val name: String)
  case object Global extends ContractOrigin("Global")
  case object Disclosed extends ContractOrigin("Disclosed")
  case object Local extends ContractOrigin("Local")

  sealed abstract class CreationPackageStatus
  case object CreationPackageVetted extends CreationPackageStatus
  case object CreationPackageUnvetted extends CreationPackageStatus
}
