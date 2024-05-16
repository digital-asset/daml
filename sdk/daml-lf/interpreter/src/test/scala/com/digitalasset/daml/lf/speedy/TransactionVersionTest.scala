// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.daml.lf.data.Ref.{IdString, PackageId, Party, TypeConName}
import com.daml.lf.language.LanguageMajorVersion.{V1, V2}
import com.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.daml.lf.speedy.SBuiltin.{SBCastAnyContract, SBFetchAny}
import com.daml.lf.speedy.SExpr.{SEMakeClo, SEValue}
import com.daml.lf.testing.parser
import com.daml.lf.transaction.{SubmittedTransaction, TransactionVersion, Versioned}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class TransactionVersionTestV1 extends TransactionVersionTest(V1)
//class TransactionVersionTestV2 extends TransactionVersionTest(V2)

class TransactionVersionTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with TableDrivenPropertyChecks {

  val helpers = new TransactionVersionTestHelpers(majorLanguageVersion)
  import helpers._

  "interface and transaction versioning" - {

    "version testing assumptions" in {
      // TODO(#17366): remove this assumption once 2.0 is introduced
      assume(majorLanguageVersion == V1)
      oldVersion should be < newVersion
      Set(
        templatePkg.languageVersion,
        interfacesPkg.languageVersion,
        implementsPkg.languageVersion,
        coImplementsPkg.languageVersion,
      ) shouldBe Set(commonVersion)
    }

    "template version > interface version" in {
      val oldPkg1 = templatePkg.copy(languageVersion = oldVersion)
      val oldPkg2 = interfacesPkg.copy(languageVersion = oldVersion)
      val newPkg1 = implementsPkg.copy(languageVersion = newVersion)
      val newPkg2 = coImplementsPkg.copy(languageVersion = newVersion)
      val pkgs = SpeedyTestLib.typeAndCompile(
        majorLanguageVersion,
        Map(
          templatePkgId -> oldPkg1,
          interfacesPkgId -> oldPkg2,
          implementsPkgId -> newPkg1,
          coImplementsPkgId -> newPkg2,
        ),
      )

      for ((templateId, interfaceId, contract) <- testData) {
        val result = evaluateBeginExercise(
          pkgs,
          templateId,
          Some(interfaceId),
          contractId,
          committers = Set(contractParty),
          controllers = Set(contractParty),
          getContract = Map(contractId -> contract),
        )

        inside(result) { case Right(transaction) =>
          transaction.version shouldBe TransactionVersion.assignNodeVersion(newVersion)
        }
      }
    }

    "template version < interface version" in {
      val oldPkg1 = implementsPkg.copy(languageVersion = oldVersion)
      val oldPkg2 = coImplementsPkg.copy(languageVersion = oldVersion)
      val newPkg1 = templatePkg.copy(languageVersion = newVersion)
      val newPkg2 = interfacesPkg.copy(languageVersion = newVersion)
      val pkgs = SpeedyTestLib.typeAndCompile(
        majorLanguageVersion,
        Map(
          templatePkgId -> newPkg1,
          interfacesPkgId -> newPkg2,
          implementsPkgId -> oldPkg1,
          coImplementsPkgId -> oldPkg2,
        ),
      )

      for ((templateId, interfaceId, contract) <- testData) {
        val result = evaluateBeginExercise(
          pkgs,
          templateId,
          Some(interfaceId),
          contractId,
          committers = Set(contractParty),
          controllers = Set(contractParty),
          getContract = Map(contractId -> contract),
        )

        inside(result) { case Right(transaction) =>
          transaction.version shouldBe TransactionVersion.assignNodeVersion(newVersion)
        }
      }
    }

    "template version == interface version" in {
      val pkgs = SpeedyTestLib.typeAndCompile(
        majorLanguageVersion,
        Map(
          templatePkgId -> templatePkg,
          interfacesPkgId -> interfacesPkg,
          implementsPkgId -> implementsPkg,
          coImplementsPkgId -> coImplementsPkg,
        ),
      )

      for ((templateId, interfaceId, contract) <- testData) {
        val result = evaluateBeginExercise(
          pkgs,
          templateId,
          Some(interfaceId),
          contractId,
          committers = Set(contractParty),
          controllers = Set(contractParty),
          getContract = Map(contractId -> contract),
        )

        inside(result) { case Right(transaction) =>
          transaction.version shouldBe TransactionVersion.assignNodeVersion(commonVersion)
        }
      }
    }
  }
}

private[lf] class TransactionVersionTestHelpers(majorLanguageVersion: LanguageMajorVersion) {

  val (commonVersion, oldVersion, newVersion) = majorLanguageVersion match {
    case V1 => (LanguageVersion.default, LanguageVersion.v1_15, LanguageVersion.v1_dev)
    case V2 =>
      (
        // TODO(#17366): Use something like languageVersion.default(V2) once available
        LanguageVersion.v2_1,
        LanguageVersion.v2_1,
        LanguageVersion.v2_dev,
      )
  }

  implicit val parserParameters: parser.ParserParameters[this.type] =
    parser.ParserParameters(
      Ref.PackageId.assertFromString("-pkg-"),
      commonVersion,
    )

  val (templatePkgId, templatePkg) =
    PackageId.assertFromString("template-pkg") -> p""" metadata ( 'template-pkg' : '1.0.0' )
        module TemplateMod {
          record @serializable Template1 = { person: Party, label: Text };
          template (this: Template1) = {
            precondition True;
            signatories (Cons @Party ['template-pkg':TemplateMod:Template1 {person} this] (Nil @Party));
            observers (Nil @Party);
            agreement "Agreement for template Template1";

            choice Destroy (self) (arg: Unit): Unit,
              controllers (Cons @Party ['template-pkg':TemplateMod:Template1 {person} this] (Nil @Party)),
              observers (Nil @Party)
              to upure @Unit ();
          };
        }
       """
  val (interfacesPkgId, interfacesPkg) =
    PackageId.assertFromString("interfaces-pkg") -> p"""  metadata ( 'interfaces-pkg' : '1.0.0' )
         module InterfacesMod {
           record @serializable EmptyInterfaceView = {};

           interface (this: Interface1) = {
             viewtype 'interfaces-pkg':InterfacesMod:EmptyInterfaceView;
             method getPerson: Party;
             choice Destroy (self) (arg: Unit): Unit,
               controllers Cons @Party [call_method @'interfaces-pkg':InterfacesMod:Interface1 getPerson this] (Nil @Party),
               observers Nil @Party
               to upure @Unit ();
           };

           interface (this: Interface2) = {
             viewtype 'interfaces-pkg':InterfacesMod:EmptyInterfaceView;
             method getPerson: Party;
             method getLabel: Text;
             choice Destroy (self) (arg: Unit): Unit,
               controllers Cons @Party [call_method @'interfaces-pkg':InterfacesMod:Interface2 getPerson this] (Nil @Party),
               observers Nil @Party
               to upure @Unit ();
           };
         }
       """
  val (implementsPkgId, implementsPkg) =
    PackageId.assertFromString("implements-pkg") -> p""" metadata ( 'implements-pkg' : '1.0.0' )
        module ImplementsMod {
          record @serializable TemplateImplements1 = { person: Party, label: Text } ;
          template (this: TemplateImplements1) = {
            precondition True;
            signatories Cons @Party ['implements-pkg':ImplementsMod:TemplateImplements1 {person} this] (Nil @Party);
            observers (Nil @Party);
            agreement "Agreement for template TemplateImplements1";
            implements 'interfaces-pkg':InterfacesMod:Interface1 {
              view = 'interfaces-pkg':InterfacesMod:EmptyInterfaceView {};
              method getPerson = 'implements-pkg':ImplementsMod:TemplateImplements1 {person} this;
            };
          };

          record @serializable TemplateImplements2 = { person: Party, label: Text } ;
          template (this: TemplateImplements2) = {
            precondition True;
            signatories Cons @Party ['implements-pkg':ImplementsMod:TemplateImplements2 {person} this] (Nil @Party);
            observers (Nil @Party);
            agreement "Agreement for template TemplateImplements2";
            implements 'interfaces-pkg':InterfacesMod:Interface2 {
              view = 'interfaces-pkg':InterfacesMod:EmptyInterfaceView {};
              method getPerson = 'implements-pkg':ImplementsMod:TemplateImplements2 {person} this;
              method getLabel = "template-implements-2";
            };
          };

          record @serializable TemplateImplements12 = { person: Party, label: Text } ;
          template (this: TemplateImplements12) = {
            precondition True;
            signatories Cons @Party ['implements-pkg':ImplementsMod:TemplateImplements12 {person} this] (Nil @Party);
            observers (Nil @Party);
            agreement "Agreement for template TemplateImplements12";
            implements 'interfaces-pkg':InterfacesMod:Interface1 {
              view = 'interfaces-pkg':InterfacesMod:EmptyInterfaceView {};
              method getPerson = 'implements-pkg':ImplementsMod:TemplateImplements12 {person} this;
            };
            implements 'interfaces-pkg':InterfacesMod:Interface2 {
              view = 'interfaces-pkg':InterfacesMod:EmptyInterfaceView {};
              method getPerson = 'implements-pkg':ImplementsMod:TemplateImplements12 {person} this;
              method getLabel = "template-implements-1-2";
            };
          };
        }
      """
  val (coImplementsPkgId, coImplementsPkg) =
    PackageId.assertFromString("coimplements-pkg") -> p""" metadata ( 'coimplements-pkg' : '1.0.0' )
        module CoImplementsMod {
          record @serializable EmptyInterfaceView = {};

          interface (this: InterfaceCoImplements1) = {
            viewtype 'coimplements-pkg':CoImplementsMod:EmptyInterfaceView;
            method getPerson: Party;
            method getLabel: Text;
            choice Destroy (self) (arg: Unit): Unit,
              controllers Cons @Party [call_method @'coimplements-pkg':CoImplementsMod:InterfaceCoImplements1 getPerson this] (Nil @Party),
              observers Nil @Party
              to upure @Unit ();
            coimplements 'template-pkg':TemplateMod:Template1 {
              view = 'coimplements-pkg':CoImplementsMod:EmptyInterfaceView {};
              method getPerson = 'template-pkg':TemplateMod:Template1 {person} this;
              method getLabel = 'template-pkg':TemplateMod:Template1 {label} this;
            };
          };
        }
       """
  val contractParty: IdString.Party = Ref.Party.assertFromString("contractParty")
  val implementsTemplateId: Ref.TypeConName =
    Ref.TypeConName.assertFromString(s"$implementsPkgId:ImplementsMod:TemplateImplements1")
  val coimplementsTemplateId: Ref.TypeConName =
    Ref.TypeConName.assertFromString(s"$templatePkgId:TemplateMod:Template1")
  val implementsInterfaceId: Ref.TypeConName =
    Ref.TypeConName.assertFromString(s"$interfacesPkgId:InterfaceMod:Interface1")
  val coimplementsInterfaceId: Ref.TypeConName =
    Ref.TypeConName.assertFromString(
      s"$coImplementsPkgId:CoImplementsMod:InterfaceCoImplements1"
    )
  val contractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val implementsContract: Versioned[Value.ContractInstance] = Versioned(
    TransactionVersion.assignNodeVersion(newVersion),
    Value.ContractInstance(
      implementsPkg.nameVersion,
      implementsTemplateId,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(contractParty),
          None -> Value.ValueText("test"),
        ),
      ),
    ),
  )
  val coimplementsContract: Versioned[Value.ContractInstance] = Versioned(
    TransactionVersion.assignNodeVersion(newVersion),
    Value.ContractInstance(
      coImplementsPkg.nameVersion,
      coimplementsTemplateId,
      Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(contractParty),
          None -> Value.ValueText("test"),
        ),
      ),
    ),
  )
  val testData = Seq(
    (implementsTemplateId, implementsInterfaceId, implementsContract),
    (coimplementsTemplateId, coimplementsInterfaceId, coimplementsContract),
  )

  def evaluateBeginExercise(
      pkgs: CompiledPackages,
      templateId: TypeConName,
      interfaceId: Option[TypeConName],
      contractId: ContractId,
      committers: Set[Party] = Set.empty,
      controllers: Set[Party] = Set.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] = {
    import SpeedyTestLib.loggingContext

    val choiceName = Ref.ChoiceName.assertFromString("Destroy")
    val choiceArg = SExpr.SEValue(SValue.SUnit)
    val speedyContractId = SExpr.SEValue(SValue.SContractId(contractId))
    val speedyControllers =
      SExpr.SEValue(SValue.SList(FrontStack.from(controllers.map(SValue.SParty))))
    val speedyObservers = SExpr.SEValue(SValue.SList(FrontStack.Empty))
    val speedyAuthorizers = SExpr.SEValue(SValue.SList(FrontStack.Empty))
    val machine =
      Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed = crypto.Hash.hashPrivateKey("TransactionVersionTest"),
        updateSE = SEMakeClo(
          Array(),
          1,
          SExpr.SELet1General(
            SBFetchAny(optTargetTemplateId = None)(speedyContractId, SEValue.None),
            SExpr.SELet1General(
              SBCastAnyContract(templateId)(
                speedyContractId,
                SExpr.SELocS(1), // result of SBFetchAny
              ),
              SExpr.SEScopeExercise(
                SBuiltin.SBUBeginExercise(
                  templateId,
                  interfaceId,
                  choiceName,
                  consuming = true,
                  byKey = false,
                  explicitChoiceAuthority = false,
                )(
                  choiceArg,
                  speedyContractId,
                  speedyControllers,
                  speedyObservers,
                  speedyAuthorizers,
                  SExpr.SELocS(1), // result of SBCastAnyContract
                )
              ),
            ),
          ),
        ),
        committers = committers,
      )

    SpeedyTestLib.buildTransaction(machine, getContract = getContract)
  }
}
