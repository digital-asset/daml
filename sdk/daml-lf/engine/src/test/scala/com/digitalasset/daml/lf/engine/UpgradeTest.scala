// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command.{ApiCommand, ApiCommands}
import com.daml.lf.data.Ref.{Identifier, Name, Party}
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.SValue
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
            record @serializable MyUnit = {};
            interface (this : Iface) = {
              viewtype Mod:MyUnit;

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

    def v2Precondition: String = v1Precondition
    def v2Signatories: String = v1Signatories
    def v2Observers: String = v1Observers
    def v2Agreement: String = v1Agreement
    def v2Key: String = v1Key
    def v2ChoiceControllers: String = v1ChoiceControllers
    def v2ChoiceObservers: String = v1ChoiceObservers
    def v2Maintainers: String = v1Maintainers

    private def templateDefinition(
        precondition: String,
        signatories: String,
        observers: String,
        agreement: String,
        key: String,
        maintainers: String,
        choiceControllers: String,
        choiceObservers: String,
    ): String =
      s"""
         |  record @serializable $templateName = { p1: Party, p2: Party };
         |  template (this: $templateName) = {
         |    precondition $precondition;
         |    signatories $signatories;
         |    observers $observers;
         |    agreement $agreement;
         |
         |    choice @nonConsuming SomeChoice (self) (u: Unit): Text
         |      , controllers (Cons @Party [Mod:${templateName} {p1} this] (Nil @Party))
         |      , observers (Nil @Party)
         |      to upure @Text "SomeChoice was called";
         |
         |    implements '$commonDefsPkgId':Mod:Iface {
         |      view = '$commonDefsPkgId':Mod:MyUnit {};
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
    )

    def clientChoices(v1PkgId: Ref.PackageId, v2PkgId: Ref.PackageId): String = {
      val v1TplQualifiedName = s"'$v1PkgId':Mod:$templateName"
      val v2TplQualifiedName = s"'$v2PkgId':Mod:$templateName"
      s"""
        |  choice @nonConsuming ExerciseLocal${templateName} (self) (u: Unit): Text
        |    , controllers (Cons @Party [Mod:Client {p} this] (Nil @Party))
        |    , observers (Nil @Party)
        |    to ubind cid: ContractId $v1TplQualifiedName <-
        |         create @$v1TplQualifiedName ($v1TplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
        |       in exercise
        |            @$v2TplQualifiedName
        |            SomeChoice
        |            (COERCE_CONTRACT_ID @$v1TplQualifiedName @$v2TplQualifiedName cid)
        |            ();
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
    override def v2Observers = s"Cons @Party [Mod:${templateName} {p1} this] (Nil @Party)"
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

  val packageMap =
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

  "test" in {

    implicit val logContext: LoggingContext = LoggingContext.ForTesting

    val participant = Ref.ParticipantId.assertFromString("participant")
    val engine = newEngine()
    val alice: Party = Party.assertFromString("Alice")
    val bob: Party = Party.assertFromString("Bob")

    val tplQualifiedName =
      Ref.QualifiedName.assertFromString(s"Mod:${ChangedObserversValid.templateName}")
    val tplId = Identifier(templateDefsV1PkgId, tplQualifiedName)
    val let = Time.Timestamp.Epoch

    val submissionSeed = crypto.Hash.hashPrivateKey("command")
    val submitters = Set(alice)
    val readAs = Set.empty[Party]

    case class ContractInfo(
        instance: VersionedContractInstance,
        signatories: Set[Party],
        observers: Set[Party],
        keyOpt: Option[GlobalKeyWithMaintainers],
    )

    @nowarn
    val createCommand =
      ApiCommand.Create(
        tplId,
        ValueRecord(
          Some(tplId),
          ImmArray(
            Some(Name.assertFromString("p1")) -> ValueParty(alice),
            Some(Name.assertFromString("p2")) -> ValueParty(bob),
          ),
        ),
      )

    val lookupContract =
      Map(
        toContractId("1") ->
          assertAsVersionedContract(
            ContractInstance(
              templateDefsV1Pkg.name,
              Identifier(templateDefsV1PkgId, tplQualifiedName),
              ValueRecord(
                Some(Identifier(templateDefsV1PkgId, tplQualifiedName)),
                ImmArray(
                  Some(Name.assertFromString("p1")) -> ValueParty(alice),
                  Some(Name.assertFromString("p2")) -> ValueParty(bob),
                ),
              ),
            )
          )
      )

    @nowarn
    val fetchCmd = speedy.Command.FetchTemplate(
      Identifier(templateDefsV1PkgId, tplQualifiedName),
      SValue.SContractId(toContractId("1")),
    )

    // @nowarn
    val exerciseCmd = ApiCommand.Exercise(
      Identifier(templateDefsV2PkgId, tplQualifiedName),
      toContractId("1"),
      Ref.ChoiceName.assertFromString("SomeChoice"),
      ValueUnit,
    )

    val interpretResult = engine
      .submit(
        packageMap = packageMap,
        packagePreference = Set(commonDefsPkgId, templateDefsV2PkgId, clientPkgId),
        submitters = submitters,
        readAs = readAs,
        cmds = ApiCommands(ImmArray(exerciseCmd), let, "test"),
        disclosures = ImmArray.empty,
        participantId = participant,
        submissionSeed = submissionSeed,
      )
      .consume(
        pcs = lookupContract,
        pkgs = lookupPackage,
        keys = Map.empty,
      )
    interpretResult shouldBe a[Right[_, _]]
  }

}
