// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command.{ApiCommand, ApiCommands, DisclosedContract}
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.engine.{Error => EE}
import com.daml.lf.interpretation.{Error => IE}
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, SubmittedTransaction, Transaction}
import com.daml.lf.value.Value._
import com.daml.logging.LoggingContext
import org.scalatest.Assertion
import org.scalatest.Inside.inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

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
  * These invalid combinations are discarded in [[TestHelper.makeApiCommand]]. For other valid combinations, this method
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
class UpgradeTest extends AnyFreeSpec with Matchers {

  private[this] implicit def parserParameters(implicit
      pkgId: Ref.PackageId
  ): ParserParameters[this.type] =
    ParserParameters(pkgId, languageVersion = LanguageVersion.Features.smartContractUpgrade)

  // A package that defines an interface, a key type, an exception, and a party to be used by
  // the packages defined below.
  val commonDefsPkgId = Ref.PackageId.assertFromString("-common-defs-id-")
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

            record @serializable Key =
              { label: Text
              , maintainers1: List Party
              , maintainers2: List Party
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
  case object ExpectUnhandledException
      extends ExpectedOutcome("should fail with an unhandled exception")

  /** An abstract class whose [[v1TemplateDefinition]], [[v2TemplateDefinition]] and [[clientChoices]] methods generate
    * LF code that define a template named [[templateName]] and test choices for that template.
    * The class is meant to be extended by concrete case objects which override some aspect of the default template,
    * for instance [[v2Observers]].
    */
  abstract class TestCase(val templateName: String, val expectedOutcome: ExpectedOutcome) {
    def v1AdditionalFields: String = ""
    def v1Precondition: String = "True"
    def v1Signatories: String = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    def v1Observers: String = "Nil @Party"
    def v1Agreement: String = """ "agreement" """
    def v1Key: String =
      s"""
         |  '$commonDefsPkgId':Mod:Key {
         |    label = "test-key",
         |    maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
         |    maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
         |  }""".stripMargin
    def v1Maintainers: String =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
    def v1ChoiceControllers: String =
      s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    def v1ChoiceObservers: String = "Nil @Party"
    def v1View: String = s"'$commonDefsPkgId':Mod:MyView { value = 0 }"

    def v2AdditionalFields: String = ""
    def v2Precondition: String = v1Precondition
    def v2Signatories: String = v1Signatories
    def v2Observers: String = v1Observers
    def v2Agreement: String = v1Agreement
    def v2Key: String = v1Key
    def v2ChoiceControllers: String = v1ChoiceControllers
    def v2ChoiceObservers: String = v1ChoiceObservers
    def v2Maintainers: String = v1Maintainers
    def v2View: String = v1View

    private def templateDefinition(
        additionalFields: String,
        precondition: String,
        signatories: String,
        observers: String,
        agreement: String,
        key: String,
        maintainers: String,
        choiceControllers: String,
        choiceObservers: String,
        view: String,
    ): String =
      s"""
         |  record @serializable $templateName = { p1: Party, p2: Party $additionalFields };
         |  template (this: $templateName) = {
         |    precondition $precondition;
         |    signatories $signatories;
         |    observers $observers;
         |    agreement $agreement;
         |
         |    choice @nonConsuming TemplateChoice (self) (u: Unit): Text
         |      , controllers (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |      , observers (Nil @Party)
         |      to upure @Text "TemplateChoice was called";
         |
         |    implements '$commonDefsPkgId':Mod:Iface {
         |      view = $view;
         |      method interfaceChoiceControllers = $choiceControllers;
         |      method interfaceChoiceObservers = $choiceObservers;
         |    };
         |
         |    key @'$commonDefsPkgId':Mod:Key ($key) ($maintainers);
         |  };""".stripMargin

    def v1TemplateDefinition: String = templateDefinition(
      v1AdditionalFields,
      v1Precondition,
      v1Signatories,
      v1Observers,
      v1Agreement,
      v1Key,
      v1Maintainers,
      v1ChoiceControllers,
      v1ChoiceObservers,
      v1View,
    )

    def v2TemplateDefinition: String = templateDefinition(
      v2AdditionalFields,
      v2Precondition,
      v2Signatories,
      v2Observers,
      v2Agreement,
      v2Key,
      v2Maintainers,
      v2ChoiceControllers,
      v2ChoiceObservers,
      v2View,
    )

    def clientChoices(
        clientPkgId: Ref.PackageId,
        v1PkgId: Ref.PackageId,
        v2PkgId: Ref.PackageId,
    ): String = {
      val clientTplQualifiedName = s"'$clientPkgId':Mod:Client"
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      val ifaceQualifiedName = s"'$commonDefsPkgId':Mod:Iface"
      val viewQualifiedName = s"'$commonDefsPkgId':Mod:MyView"
      s"""
        |  choice @nonConsuming ExerciseNoCatchLocal${templateName} (self) (u: Unit): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
        |       in exercise
        |            @$v2TplQualifiedName
        |            TemplateChoice
        |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
        |            ();
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
        |         ();
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
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
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
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
        |       in exercise_by_key
        |            @$v2TplQualifiedName
        |            TemplateChoice
        |            ('$commonDefsPkgId':Mod:Key {
        |                 label = "test-key",
        |                 maintainers1 = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)),
        |                 maintainers2 = (Cons @Party ['$commonDefsPkgId':Mod:bob] (Nil @Party)) })
        |            ();
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
        |  choice @nonConsuming ExerciseByKeyNoCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to exercise_by_key
        |         @$v2TplQualifiedName
        |         TemplateChoice
        |         key
        |         ();
        |
        |  choice @nonConsuming ExerciseByKeyAttemptCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key)
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
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
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
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
        |       in ubind pair:<contract: $v2TplQualifiedName, contractId: ContractId $v2TplQualifiedName> <-
        |              fetch_by_key
        |                @$v2TplQualifiedName
        |                ('$commonDefsPkgId':Mod:Key {
        |                     label = "test-key",
        |                     maintainers1 = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)),
        |                     maintainers2 = (Cons @Party ['$commonDefsPkgId':Mod:bob] (Nil @Party)) })
        |          in upure @$v2TplQualifiedName (pair).contract;
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
        |  choice @nonConsuming FetchByKeyNoCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key)
        |        : $v2TplQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind pair:<contract: $v2TplQualifiedName, contractId: ContractId $v2TplQualifiedName> <-
        |          fetch_by_key
        |            @$v2TplQualifiedName
        |            key
        |       in upure @$v2TplQualifiedName (pair).contract;
        |
        |  choice @nonConsuming FetchByKeyAttemptCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key)
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
        |  choice @nonConsuming FetchInterfaceNoCatchLocal${templateName} (self) (u: Unit): $viewQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind
        |         cid: ContractId $v1TplQualifiedName <-
        |           create
        |             @$v1TplQualifiedName
        |             ($v1TplQualifiedName
        |                 { p1 = '$commonDefsPkgId':Mod:alice
        |                 , p2 = '$commonDefsPkgId':Mod:bob
        |                 });
        |         iface: $ifaceQualifiedName <- fetch_interface
        |            @$ifaceQualifiedName
        |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
        |         in upure @$viewQualifiedName (view_interface @$ifaceQualifiedName iface);
        |
        |  choice @nonConsuming FetchInterfaceAttemptCatchLocal${templateName} (self) (u: Unit): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to try @Text
        |         ubind _:$viewQualifiedName <-
        |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchLocal${templateName} self ()
        |         in upure @Text "no exception was caught"
        |       catch
        |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
        |
        |  choice @nonConsuming FetchInterfaceNoCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName)
        |        : $viewQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind iface: $ifaceQualifiedName <- fetch_interface
        |         @$ifaceQualifiedName
        |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
        |       in upure @$viewQualifiedName (view_interface @$ifaceQualifiedName iface);
        |
        |  choice @nonConsuming FetchInterfaceAttemptCatchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to try @Text
        |         ubind _:$viewQualifiedName <-
        |             exercise @$clientTplQualifiedName FetchInterfaceNoCatchGlobal${templateName} self cid
        |         in upure @Text "no exception was caught"
        |       catch
        |         e -> Some @(Update Text) (upure @Text "unexpected: some exception was caught");
        |
        |  choice @nonConsuming LookupByKeyNoCatchLocal${templateName} (self) (u: Unit): Option (ContractId $v2TplQualifiedName)
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create
        |           @$v1TplQualifiedName
        |           ($v1TplQualifiedName
        |               { p1 = '$commonDefsPkgId':Mod:alice
        |               , p2 = '$commonDefsPkgId':Mod:bob
        |               })
        |       in lookup_by_key
        |            @$v2TplQualifiedName
        |            ('$commonDefsPkgId':Mod:Key {
        |                 label = "test-key",
        |                 maintainers1 = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)),
        |                 maintainers2 = (Cons @Party ['$commonDefsPkgId':Mod:bob] (Nil @Party)) });
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
        |  choice @nonConsuming LookupByKeyNoCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key)
        |        : Option (ContractId $v2TplQualifiedName)
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to lookup_by_key
        |         @$v2TplQualifiedName
        |         key;
        |
        |  choice @nonConsuming LookupByKeyAttemptCatchGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key)
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

  case object UnchangedPrecondition extends TestCase("UnchangedPrecondition", ExpectSuccess) {
    override def v1Precondition = "True"
    override def v2Precondition = "case () of () -> True"
  }

  case object ChangedPrecondition
      extends TestCase("ChangedPrecondition", ExpectPreconditionViolated) {
    override def v1Precondition = "True"
    override def v2Precondition = "False"
  }

  case object ThrowingPrecondition
      extends TestCase("ThrowingPrecondition", ExpectUnhandledException) {
    override def v1Precondition = "True"
    override def v2Precondition =
      s"""throw @Bool @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Precondition"})"""
  }

  case object UnchangedSignatories extends TestCase("UnchangedSignatories", ExpectSuccess) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"case () of () -> Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
  }

  case object ChangedSignatories extends TestCase("ChangedSignatories", ExpectUpgradeError) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"Cons @Party [Mod:${templateName} {p1} this, Mod:${templateName} {p2} this] (Nil @Party)"
  }

  case object ThrowingSignatories
      extends TestCase("ThrowingSignatories", ExpectUnhandledException) {
    override def v1Signatories = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
    override def v2Signatories =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Signatories"})"""
  }

  case object UnchangedObservers extends TestCase("UnchangedObservers", ExpectSuccess) {
    override def v1Observers = "Nil @Party"
    override def v2Observers = "case () of () -> Nil @Party"
  }

  case object ChangedObservers extends TestCase("ChangedObservers", ExpectUpgradeError) {
    override def v1Observers = "Nil @Party"
    override def v2Observers = s"Cons @Party [Mod:${templateName} {p2} this] (Nil @Party)"
  }

  case object ThrowingObservers extends TestCase("ThrowingObservers", ExpectUnhandledException) {
    override def v1Observers = "Nil @Party"
    override def v2Observers =
      s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Observers"})"""
  }

  case object UnchangedAgreement extends TestCase("UnchangedAgreement", ExpectSuccess) {
    override def v1Agreement = """ "agreement" """
    override def v2Agreement = """ case () of () -> "agreement" """
  }

  case object ChangedAgreement extends TestCase("ChangedAgreement", ExpectSuccess) {
    override def v1Agreement = """ "agreement" """
    override def v2Agreement = """ "text changed, but we don't care" """
  }

  case object ThrowingAgreement extends TestCase("ThrowingAgreement", ExpectUnhandledException) {
    override def v1Agreement = """ "agreement" """
    override def v2Agreement =
      s"""throw @Text @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Agreement"})"""
  }

  case object UnchangedKey extends TestCase("UnchangedKey", ExpectSuccess) {
    override def v1Key = s"""
                            |  '$commonDefsPkgId':Mod:Key {
                            |    label = "test-key",
                            |    maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s""" case () of () ->
                            |    '$commonDefsPkgId':Mod:Key {
                            |      label = "test-key",
                            |      maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |      maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |    }""".stripMargin
  }

  case object ChangedKey extends TestCase("ChangedKey", ExpectUpgradeError) {
    override def v1Key = s"""
                            |  '$commonDefsPkgId':Mod:Key {
                            |    label = "test-key",
                            |    maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key = s""" case () of () ->
                            |    '$commonDefsPkgId':Mod:Key {
                            |      label = "test-key-2",
                            |      maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |      maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |    }""".stripMargin
  }

  case object ThrowingKey extends TestCase("ThrowingKey", ExpectUnhandledException) {
    override def v1Key = s"""
                            |  '$commonDefsPkgId':Mod:Key {
                            |    label = "test-key",
                            |    maintainers1 = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)),
                            |    maintainers2 = (Cons @Party [Mod:${templateName} {p2} this] (Nil @Party))
                            |  }""".stripMargin
    override def v2Key =
      s"""throw @'$commonDefsPkgId':Mod:Key @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Key"})"""
  }

  case object UnchangedMaintainers extends TestCase("UnchangedMaintainers", ExpectSuccess) {
    override def v1Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
    override def v2Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> case () of () -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
  }

  case object ChangedMaintainers extends TestCase("ChangedMaintainers", ExpectUpgradeError) {
    override def v1Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
    override def v2Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers2} key)"
  }

  case object ThrowingMaintainers
      extends TestCase("ThrowingMaintainers", ExpectUnhandledException) {
    override def v1Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
    override def v2Maintainers =
      s"""throw @('$commonDefsPkgId':Mod:Key -> List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Maintainers"})"""
  }

  case object ThrowingMaintainersBody
      extends TestCase("ThrowingMaintainersBody", ExpectUnhandledException) {
    override def v1Maintainers =
      s"\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers1} key)"
    override def v2Maintainers =
      s"""\\(key: '$commonDefsPkgId':Mod:Key) -> throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "MaintainersBody"})"""
  }

  case object AdditionalTemplateArg extends TestCase("AdditionalTemplateArg", ExpectSuccess) {
    override def v1AdditionalFields: String = ""
    override def v2AdditionalFields: String = ", extra: Option Unit"
  }

  val testCases: Seq[TestCase] = List(
    UnchangedPrecondition,
    ChangedPrecondition,
    ThrowingPrecondition,
    UnchangedSignatories,
    ChangedSignatories,
    ThrowingSignatories,
    UnchangedObservers,
    ChangedObservers,
    ThrowingObservers,
    UnchangedAgreement,
    ChangedAgreement,
    ThrowingAgreement,
    ChangedKey,
    UnchangedKey,
    ThrowingKey,
    ChangedMaintainers,
    UnchangedMaintainers,
    ThrowingMaintainers,
    ThrowingMaintainersBody,
    AdditionalTemplateArg,
  )

  val templateDefsPkgName = Ref.PackageName.assertFromString("-template-defs-")

  /** A package that defines templates called Precondition, Signatories, ... whose metadata should evaluate without
    * throwing exceptions.
    */
  val templateDefsV1PkgId = Ref.PackageId.assertFromString("-template-defs-v1-id-")
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
  val templateDefsV2PkgId = Ref.PackageId.assertFromString("-template-defs-v2-id-")
  val templateDefsV2ParserParams = parserParameters(templateDefsV2PkgId)
  val templateDefsV2Pkg =
    p"""metadata ( '$templateDefsPkgName' : '2.0.0' )
          module Mod {
            ${testCases.map(_.v2TemplateDefinition).mkString("\n")}
          }
      """ (templateDefsV2ParserParams)

  val clientPkgId = Ref.PackageId.assertFromString("-client-id-")
  val clientParserParams = parserParameters(clientPkgId)
  val clientPkg = {
    val choices = testCases
      .map(_.clientChoices(clientPkgId, templateDefsV1PkgId, templateDefsV2PkgId))
    p"""metadata ( '-client-' : '1.0.0' )
            module Mod {
              record @serializable Client = { p: Party };
              template (this: Client) = {
                precondition True;
                signatories Cons @Party [Mod:Client {p} this] (Nil @Party);
                observers Nil @Party;
                agreement "agreement";

                ${choices.mkString("\n")}
              };
            }
        """ (clientParserParams)
  }

  val lookupPackage: Map[Ref.PackageId, Ast.Package] = Map(
    commonDefsPkgId -> commonDefsPkg,
    templateDefsV1PkgId -> templateDefsV1Pkg,
    templateDefsV2PkgId -> templateDefsV2Pkg,
    clientPkgId -> clientPkg,
  )

  val packageMap: Map[PackageId, (PackageName, Ref.PackageVersion)] =
    lookupPackage.view.mapValues(pkg => (pkg.name.get, pkg.metadata.get.version)).toMap

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
    * - [[makeApiCommand]], which generates an API command for a given operation, catch behavior, entry point,
    *   and contract origin.
    * - [[execute]], which executes a command against a fresh engine seeded with the v1 contract.
    */
  class TestHelper(templateName: String) {

    implicit val logContext: LoggingContext = LoggingContext.ForTesting

    val alice: Party = Party.assertFromString("Alice")
    val bob: Party = Party.assertFromString("Bob")

    val clientTplId: Identifier =
      Identifier(clientPkgId, Ref.QualifiedName.assertFromString("Mod:Client"))
    val ifaceId: Identifier =
      Identifier(commonDefsPkgId, Ref.QualifiedName.assertFromString("Mod:Iface"))
    val tplQualifiedName: QualifiedName = Ref.QualifiedName.assertFromString(s"Mod:$templateName")
    val v1TplId: Identifier = Identifier(templateDefsV1PkgId, tplQualifiedName)
    val v2TplId: Identifier = Identifier(templateDefsV2PkgId, tplQualifiedName)

    val clientContractId: ContractId = toContractId("client")
    val globalContractId: ContractId = toContractId("1")

    val clientContract: VersionedContractInstance = assertAsVersionedContract(
      ContractInstance(
        clientPkg.name,
        clientTplId,
        ValueRecord(
          Some(clientTplId),
          ImmArray(
            Some(Name.assertFromString("p")) -> ValueParty(alice)
          ),
        ),
      )
    )

    val globalContractArg: ValueRecord = ValueRecord(
      Some(v1TplId),
      ImmArray(
        Some(Name.assertFromString("p1")) -> ValueParty(alice),
        Some(Name.assertFromString("p2")) -> ValueParty(bob),
      ),
    )

    val globalContract: VersionedContractInstance = assertAsVersionedContract(
      ContractInstance(
        templateDefsV1Pkg.name,
        v1TplId,
        globalContractArg,
      )
    )

    val globalContractSKey: SValue = SValue.SRecord(
      Ref.Identifier.assertFromString(s"$commonDefsPkgId:Mod:Key"),
      ImmArray(
        Ref.Name.assertFromString("label"),
        Ref.Name.assertFromString("maintainers1"),
        Ref.Name.assertFromString("maintainers2"),
      ),
      ArrayList(
        SValue.SText("test-key"),
        SValue.SList(FrontStack(SValue.SParty(alice))),
        SValue.SList(FrontStack(SValue.SParty(bob))),
      ),
    )

    val globalContractKey: GlobalKeyWithMaintainers = GlobalKeyWithMaintainers.assertBuild(
      v1TplId,
      globalContractSKey.toUnnormalizedValue,
      Set(alice),
      KeyPackageName(Some(templateDefsPkgName), templateDefsV1Pkg.languageVersion),
    )

    val globalContractDisclosure: DisclosedContract = DisclosedContract(
      templateId = v1TplId,
      contractId = globalContractId,
      argument = globalContractArg,
      keyHash = Some(
        crypto.Hash.assertHashContractKey(
          v1TplId,
          globalContractSKey.toUnnormalizedValue.asInstanceOf[ValueRecord],
          KeyPackageName(Some(templateDefsPkgName), templateDefsV1Pkg.languageVersion),
        )
      ),
    )

    def toContractId(s: String): ContractId =
      ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

    def makeApiCommand(
        operation: Operation,
        catchBehavior: CatchBehavior,
        entryPoint: EntryPoint,
        contractOrigin: ContractOrigin,
    ): Option[ApiCommand] = {

      (operation, catchBehavior, entryPoint, contractOrigin) match {
        case (_, _, Command, Local) =>
          None // Local contracts cannot be exercised by commands
        case (Fetch | FetchInterface | FetchByKey | LookupByKey, _, Command, _) =>
          None // There are no fetch* or lookupByKey commands
        case (Exercise, _, Command, Global | Disclosed) =>
          Some(
            ApiCommand.Exercise(
              v2TplId,
              globalContractId,
              Ref.ChoiceName.assertFromString("TemplateChoice"),
              ValueUnit,
            )
          )
        case (ExerciseInterface, _, Command, Global | Disclosed) =>
          Some(
            ApiCommand.Exercise(
              ifaceId,
              globalContractId,
              Ref.ChoiceName.assertFromString("InterfaceChoice"),
              ValueUnit,
            )
          )
        case (ExerciseByKey, _, Command, Global | Disclosed) =>
          Some(
            ApiCommand.ExerciseByKey(
              v2TplId,
              globalContractSKey.toUnnormalizedValue,
              Ref.ChoiceName.assertFromString("TemplateChoice"),
              ValueUnit,
            )
          )
        case (_, _, ChoiceBody, Global | Disclosed) =>
          Some(
            ApiCommand.Exercise(
              clientTplId,
              clientContractId,
              Ref.ChoiceName.assertFromString(
                s"${operation.name}${catchBehavior.name}Global${templateName}"
              ),
              operation match {
                case Fetch | FetchInterface | Exercise | ExerciseInterface =>
                  ValueContractId(globalContractId)
                case FetchByKey | LookupByKey | ExerciseByKey =>
                  globalContractSKey.toUnnormalizedValue
              },
            )
          )
        case (_, _, ChoiceBody, Local) =>
          Some(
            ApiCommand.Exercise(
              clientTplId,
              clientContractId,
              Ref.ChoiceName.assertFromString(
                s"${operation.name}${catchBehavior.name}Local${templateName}"
              ),
              ValueUnit,
            )
          )
      }
    }

    def newEngine() = new Engine(
      EngineConfig(allowedLanguageVersions =
        language.LanguageVersion.AllVersions(LanguageMajorVersion.V1)
      )
    )

    def execute(
        apiCommand: ApiCommand,
        contractOrigin: ContractOrigin,
    ): Either[Error, (SubmittedTransaction, Transaction.Metadata)] = {

      val participant = Ref.ParticipantId.assertFromString("participant")
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
          val keyMap = Map(globalContractKey.globalKey -> globalContractId)
          ((kwm: GlobalKeyWithMaintainers) => keyMap.get(kwm.globalKey)).unlift
        case _ => PartialFunction.empty
      }

      newEngine()
        .submit(
          packageMap = packageMap,
          packagePreference = Set(commonDefsPkgId, templateDefsV2PkgId, clientPkgId),
          submitters = submitters,
          readAs = readAs,
          cmds = ApiCommands(ImmArray(apiCommand), Time.Timestamp.Epoch, "test"),
          disclosures = disclosures,
          participantId = participant,
          submissionSeed = submissionSeed,
        )
        .consume(
          pcs = lookupContractById,
          pkgs = lookupPackage,
          keys = lookupContractByKey,
        )
    }
  }

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
      case ExpectPreconditionViolated =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.TemplatePreconditionViolated]
        }
      case ExpectUnhandledException =>
        inside(result) { case Left(EE.Interpretation(EE.Interpretation.DamlException(error), _)) =>
          error shouldBe a[IE.UnhandledException]
        }
    }
  }

  // This is the main loop of the test: for every combination of test case, operation, catch behavior, entry point, and
  // contract origin, we generate an API command, execute it, and check that the result matches the expected outcome.
  for (template <- testCases)
    template.templateName - {
      val testHelper = new TestHelper(template.templateName)
      for (operation <- operations) {
        operation.name - {
          for (catchBehavior <- catchBehaviors)
            catchBehavior.name - {
              for (entryPoint <- entryPoints) {
                entryPoint.name - {
                  for (contractOrigin <- contractOrigins) {
                    contractOrigin.name - {
                      testHelper
                        .makeApiCommand(operation, catchBehavior, entryPoint, contractOrigin)
                        .foreach { apiCommand =>
                          template.expectedOutcome.description in {
                            assertResultMatchesExpectedOutcome(
                              testHelper.execute(apiCommand, contractOrigin),
                              template.expectedOutcome,
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
