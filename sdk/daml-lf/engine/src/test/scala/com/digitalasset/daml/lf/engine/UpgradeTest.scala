// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command.{ApiCommand, ApiCommands, DisclosedContract}
import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref, Time}
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.GlobalKeyWithMaintainers
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.lf.value.Value._
import com.daml.logging.LoggingContext
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.annotation.nowarn

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

            record @serializable Key = { label: Text, maintainers: List Party };

            record @serializable Ex = { message: Text } ;
            exception Ex = {
              message \(e: Mod:Ex) -> Mod:Ex {message} e
            };

            val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
            val alice : Party = Mod:mkParty "Alice";
            val bob : Party = Mod:mkParty "Bob";
          }
      """ (parserParameters(commonDefsPkgId))

  /** An abstract class whose [[templateDefinition]] method generates LF code that defines a template named
    * [[templateName]].
    * The class is meant to be extended by concrete case objects which override one the metadata's expressions with an
    * expression that throws an exception.
    */
  abstract class TemplateGenerator(val templateName: String) {
    def v1Precondition: String = """True"""
    def v1Signatories: String = s"""Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"""
    def v1Observers: String = """Nil @Party"""
    def v1Agreement: String = """"agreement""""
    def v1Key: String =
      s"""
         |  '$commonDefsPkgId':Mod:Key {
         |    label = "test-key",
         |    maintainers = (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |  }""".stripMargin
    def v1Maintainers: String =
      s"""\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers} key)"""
    def v1ChoiceControllers: String =
      s"""Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"""
    def v1ChoiceObservers: String = """Nil @Party"""
    def v1View: String = s"'$commonDefsPkgId':Mod:MyView { value = 0 }"

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
         |  record @serializable $templateName = { p1: Party, p2: Party };
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

    def clientChoices(v1PkgId: Ref.PackageId, v2PkgId: Ref.PackageId): String = {
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      val ifaceQualifiedName = s"'$commonDefsPkgId':Mod:Iface"
      val viewQualifiedName = s"'$commonDefsPkgId':Mod:MyView"
      s"""
        |  choice @nonConsuming ExerciseLocal${templateName} (self) (u: Unit): Text
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
        |  choice @nonConsuming ExerciseGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to exercise
        |         @$v2TplQualifiedName
        |         TemplateChoice
        |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
        |         ();
        |
        |  choice @nonConsuming ExerciseInterfaceLocal${templateName} (self) (u: Unit): Text
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
        |  choice @nonConsuming ExerciseInterfaceGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to exercise_interface
        |         @$ifaceQualifiedName
        |         InterfaceChoice
        |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
        |         ();
        |
        |  choice @nonConsuming ExerciseByKeyLocal${templateName} (self) (u: Unit): Text
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
        |                 maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)) })
        |            ();
        |
        |  choice @nonConsuming ExerciseByKeyGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to exercise_by_key
        |         @$v2TplQualifiedName
        |         TemplateChoice
        |         key
        |         ();
        |
        |  choice @nonConsuming FetchLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
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
        |  choice @nonConsuming FetchGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): $v2TplQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to fetch_template
        |         @$v2TplQualifiedName
        |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid);
        |
        |  choice @nonConsuming FetchByKeyLocal${templateName} (self) (u: Unit): $v2TplQualifiedName
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
        |                     maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)) })
        |          in upure @$v2TplQualifiedName (pair).contract;
        |
        |  choice @nonConsuming FetchByKeyGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key): $v2TplQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind pair:<contract: $v2TplQualifiedName, contractId: ContractId $v2TplQualifiedName> <-
        |          fetch_by_key
        |            @$v2TplQualifiedName
        |            key
        |       in upure @$v2TplQualifiedName (pair).contract;
        |
        |  choice @nonConsuming FetchInterfaceLocal${templateName} (self) (u: Unit): $viewQualifiedName
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
        |  choice @nonConsuming FetchInterfaceGlobal${templateName} (self) (cid: ContractId $v1TplQualifiedName): $viewQualifiedName
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind iface: $ifaceQualifiedName <- fetch_interface
        |         @$ifaceQualifiedName
        |         (COERCE_CONTRACT_ID @$v1TplQualifiedName @$ifaceQualifiedName cid)
        |       in upure @$viewQualifiedName (view_interface @$ifaceQualifiedName iface);
        |
        |  choice @nonConsuming LookupByKeyLocal${templateName} (self) (u: Unit): Option (ContractId $v2TplQualifiedName)
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
        |                 maintainers = (Cons @Party ['$commonDefsPkgId':Mod:alice] (Nil @Party)) });
        |
        |  choice @nonConsuming LookupByKeyGlobal${templateName} (self) (key: '$commonDefsPkgId':Mod:Key): Option (ContractId $v2TplQualifiedName)
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to lookup_by_key
        |         @$v2TplQualifiedName
        |         key;
        |""".stripMargin
    }
  }

  case object ChangedPreconditionValid extends TemplateGenerator("ChangedPreconditionValid") {
    override def v1Precondition = "True"
    override def v2Precondition = "EQUAL @Int64 1 1"
  }

  case object ChangedPreconditionInvalid extends TemplateGenerator("ChangedPreconditionInvalid") {
    override def v1Precondition = "True"
    override def v2Precondition = "False"
  }

  case object ChangedObserversValid extends TemplateGenerator("ChangedObserversValid") {
    override def v1Observers = "Nil @Party"
    override def v2Observers = "case () of () -> Nil @Party"
  }

  case object ChangedObserversInvalid extends TemplateGenerator("ChangedObserversInvalid") {
    override def v1Observers = "Nil @Party"
    override def v2Observers = s"Cons @Party [Mod:${templateName} {p2} this] (Nil @Party)"
  }

  val templateGenerators: Seq[TemplateGenerator] = List(
    ChangedPreconditionValid,
    ChangedPreconditionInvalid,
    ChangedObserversValid,
    ChangedObserversInvalid,
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
            ${templateGenerators.map(_.v1TemplateDefinition).mkString("\n")}
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
            ${templateGenerators.map(_.v2TemplateDefinition).mkString("\n")}
          }
      """ (templateDefsV2ParserParams)

  val clientPkgId = Ref.PackageId.assertFromString("-client-id-")
  val clientParserParams = parserParameters(clientPkgId)
  val clientPkg = {
    val choices = templateGenerators
      .map(_.clientChoices(templateDefsV1PkgId, templateDefsV2PkgId))
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

  //  val compiledPackages: PureCompiledPackages =
//    PureCompiledPackages.assertBuild(
//      Map(
//        commonDefsPkgId -> commonDefsPkg,
//        templateDefsV1PkgId -> templateDefsV1Pkg,
//        templateDefsV2PkgId -> templateDefsV2Pkg,
//      ),
//      // TODO: revert to the default compiler config once it supports upgrades
//      Compiler.Config.Dev(LanguageMajorVersion.V1),
//    )
//
//  println(compiledPackages)

  val lookupPackage: Map[Ref.PackageId, Ast.Package] = Map(
    commonDefsPkgId -> commonDefsPkg,
    templateDefsV1PkgId -> templateDefsV1Pkg,
    templateDefsV2PkgId -> templateDefsV2Pkg,
    clientPkgId -> clientPkg,
  )

  val packageMap: Map[PackageId, (PackageName, Ref.PackageVersion)] =
    lookupPackage.view.mapValues(pkg => (pkg.name.get, pkg.metadata.get.version)).toMap

  // println(lookupPackage)

  def newEngine() =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = language.LanguageVersion.AllVersions(LanguageMajorVersion.V1)
      )
    )

  def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))

  sealed abstract class Operation(val name: String)
  case object Exercise extends Operation("exercise")
  case object ExerciseByKey extends Operation("exerciseByKey")
  case object ExerciseInterface extends Operation("exerciseInterface")
  case object Fetch extends Operation("fetch")
  case object FetchByKey extends Operation("fetch")
  case object FetchInterface extends Operation("fetchInterface")
  case object LookupByKey extends Operation("lookupByKey")

  sealed abstract class EntryPoint(val name: String)
  case object Command extends EntryPoint("command")
  case object ChoiceBody extends EntryPoint("choiceBody")

  sealed abstract class ContractOrigin(val name: String)
  case object Global extends ContractOrigin("global")
  case object Disclosed extends ContractOrigin("disclosed")
  case object Local extends ContractOrigin("local")

  for (templateGenerator <- templateGenerators) {
    templateGenerator.templateName - {}
  }

  "test" in {

    implicit val logContext: LoggingContext = LoggingContext.ForTesting

    val template = ChangedObserversInvalid

    val participant = Ref.ParticipantId.assertFromString("participant")
    val engine = newEngine()
    val alice: Party = Party.assertFromString("Alice")
    val bob: Party = Party.assertFromString("Bob")

    val tplQualifiedName =
      Ref.QualifiedName.assertFromString(s"Mod:${template.templateName}")
    val v1TplId = Identifier(templateDefsV1PkgId, tplQualifiedName)
    val v2TplId = Identifier(templateDefsV2PkgId, tplQualifiedName)

    val let = Time.Timestamp.Epoch

    val submissionSeed = crypto.Hash.hashPrivateKey("command")
    val submitters = Set(alice)
    val readAs = Set.empty[Party]

    val clientTplId = Identifier(clientPkgId, Ref.QualifiedName.assertFromString("Mod:Client"))

    val ifaceId = Identifier(commonDefsPkgId, Ref.QualifiedName.assertFromString("Mod:Iface"))

//    @nowarn
//    val createCommand =
//      ApiCommand.Create(
//        v1TplId,
//        ValueRecord(
//          Some(v1TplId),
//          ImmArray(
//            Some(Name.assertFromString("p1")) -> ValueParty(alice),
//            Some(Name.assertFromString("p2")) -> ValueParty(bob),
//          ),
//        ),
//      )

    val clientContractId = toContractId("client")

    val globalContractId = toContractId("1")
    val globalContractArg = ValueRecord(
      Some(v1TplId),
      ImmArray(
        Some(Name.assertFromString("p1")) -> ValueParty(alice),
        Some(Name.assertFromString("p2")) -> ValueParty(bob),
      ),
    )

    val lookupContract =
      Map(
        clientContractId ->
          assertAsVersionedContract(
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
          ),
        globalContractId ->
          assertAsVersionedContract(
            ContractInstance(
              templateDefsV1Pkg.name,
              v1TplId,
              globalContractArg,
            )
          ),
      )

    val key = SValue.SRecord(
      Ref.Identifier.assertFromString(s"$commonDefsPkgId:Mod:Key"),
      ImmArray(
        Ref.Name.assertFromString("label"),
        Ref.Name.assertFromString("maintainers"),
      ),
      ArrayList(
        SValue.SText("test-key"),
        SValue.SList(FrontStack(SValue.SParty(alice))),
      ),
    )

    val globalKey = GlobalKeyWithMaintainers.assertBuild(
      v1TplId,
      key.toUnnormalizedValue,
      Set(alice),
      KeyPackageName(Some(templateDefsPkgName), templateDefsV1Pkg.languageVersion),
    )

    @nowarn
    val globalContractDisclosure = DisclosedContract(
      templateId = v1TplId,
      contractId = globalContractId,
      argument = globalContractArg,
      keyHash = Some(
        crypto.Hash.assertHashContractKey(
          v1TplId,
          key.toUnnormalizedValue.asInstanceOf[ValueRecord],
          KeyPackageName(Some(templateDefsPkgName), templateDefsV1Pkg.languageVersion),
        )
      ),
    )

    // @nowarn
    val exerciseCmdGlobal = ApiCommand.Exercise(
      v2TplId,
      globalContractId,
      Ref.ChoiceName.assertFromString("TemplateChoice"),
      ValueUnit,
    )

    @nowarn
    val exerciseCmdLocal = ApiCommand.CreateAndExercise(
      v2TplId,
      ValueRecord(
        Some(v2TplId),
        ImmArray(
          Some(Name.assertFromString("p1")) -> ValueParty(alice),
          Some(Name.assertFromString("p2")) -> ValueParty(bob),
        ),
      ),
      Ref.ChoiceName.assertFromString("TemplateChoice"),
      ValueUnit,
    )

    @nowarn
    val exerciseInterfaceCmdGlobal = ApiCommand.Exercise(
      ifaceId,
      globalContractId,
      Ref.ChoiceName.assertFromString("InterfaceChoice"),
      ValueUnit,
    )

    @nowarn
    val exerciseByKeyCmdGlobal = ApiCommand.ExerciseByKey(
      v2TplId,
      key.toUnnormalizedValue,
      Ref.ChoiceName.assertFromString("TemplateChoice"),
      ValueUnit,
    )

    @nowarn
    val fetchChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchGlobal${template.templateName}"),
      ValueContractId(globalContractId),
    )

    @nowarn
    val fetchChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val fetchInterfaceChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchInterfaceGlobal${template.templateName}"),
      ValueContractId(globalContractId),
    )

    @nowarn
    val fetchInterfaceChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchInterfaceLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val fetchByKeyChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchByKeyGlobal${template.templateName}"),
      key.toUnnormalizedValue,
    )

    @nowarn
    val fetchByKeyChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"FetchByKeyLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val lookupByKeyChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"LookupByKeyGlobal${template.templateName}"),
      key.toUnnormalizedValue,
    )

    @nowarn
    val lookupByKeyChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"LookupByKeyLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val exerciseChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseGlobal${template.templateName}"),
      ValueContractId(globalContractId),
    )

    @nowarn
    val exerciseChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val exerciseByKeyChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseByKeyGlobal${template.templateName}"),
      key.toUnnormalizedValue,
    )

    @nowarn
    val exerciseByKeyChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseByKeyLocal${template.templateName}"),
      ValueUnit,
    )

    @nowarn
    val exerciseInterfaceChoiceBodyGlobal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseInterfaceGlobal${template.templateName}"),
      ValueContractId(globalContractId),
    )

    @nowarn
    val exerciseInterfaceChoiceBodyLocal = ApiCommand.Exercise(
      clientTplId,
      clientContractId,
      Ref.ChoiceName.assertFromString(s"ExerciseInterfaceLocal${template.templateName}"),
      ValueUnit,
    )

    val interpretResult = engine
      .submit(
        packageMap = packageMap,
        packagePreference = Set(commonDefsPkgId, templateDefsV2PkgId, clientPkgId),
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(ImmArray(exerciseCmdGlobal), let, "test"),
        disclosures = ImmArray.empty, // ImmArray(globalContractDisclosure),
        participantId = participant,
        submissionSeed = submissionSeed,
      )
      .consume(
        pcs = lookupContract,
        pkgs = lookupPackage,
        keys = Map(globalKey -> globalContractId),
      )
    interpretResult shouldBe a[Right[_, _]]
  }
}
