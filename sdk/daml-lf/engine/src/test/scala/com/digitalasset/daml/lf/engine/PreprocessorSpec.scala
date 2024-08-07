// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageRef, Party}
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.{ArrayList, Command, DisclosedContract, SValue}
import com.digitalasset.daml.lf.value.Value.{
  ContractId,
  ValueInt64,
  ValueList,
  ValueParty,
  ValueRecord,
}
import org.scalatest.{Assertion, Inside, Inspectors}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
  defaultPackageId => _,
  _,
}
import com.digitalasset.daml.lf.transaction.{
  FatContractInstanceImpl,
  GlobalKeyWithMaintainers,
  TransactionVersion,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.speedy.Compiler

import scala.collection.immutable

class PreprocessorSpecV2 extends PreprocessorSpec(LanguageMajorVersion.V2)

class PreprocessorSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Inside
    with Matchers
    with Inspectors
    with TableDrivenPropertyChecks {

  val helpers = new PreprocessorSpecHelpers(majorLanguageVersion)
  import helpers._

  val compilerConfig = Compiler.Config.Dev(majorLanguageVersion)

  "preprocessor" should {
    "returns correct result when resuming" in {
      val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages(compilerConfig))
      val intermediaryResult = preprocessor
        .translateValue(
          Ast.TTyCon("Mod:WithoutKey"),
          ValueRecord("", ImmArray("owners" -> parties, "data" -> ValueInt64(42))),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(pkgs = pkgs)
      finalResult shouldBe a[Right[_, _]]
    }

    "returns correct error when resuming" in {
      val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages(compilerConfig))
      val intermediaryResult = preprocessor
        .translateValue(
          Ast.TTyCon("Mod:WithoutKey"),
          ValueRecord(
            "",
            ImmArray("owners" -> parties, "wrong_field" -> ValueInt64(42)),
          ),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(pkgs = pkgs)
      inside(finalResult) { case Left(Error.Preprocessing(error)) =>
        error shouldBe a[Error.Preprocessing.TypeMismatch]
      }
    }

    val cmdsByPackageName = {
      val recordFields = Value.ValueRecord(
        None,
        ImmArray(
          None -> parties,
          None -> Value.ValueInt64(42L),
        ),
      )
      Table(
        "commands",
        ApiCommand.Create(
          templateRef = withoutKeyTmplRef,
          argument = recordFields,
        ),
        ApiCommand.Exercise(
          typeRef = withoutKeyTmplRef,
          contractId = contractId,
          choiceId = choiceId,
          argument = Value.ValueUnit,
        ),
        ApiCommand.ExerciseByKey(
          templateRef = withKeyTmplRef,
          contractKey = parties,
          choiceId = choiceId,
          argument = Value.ValueUnit,
        ),
        ApiCommand.CreateAndExercise(
          templateRef = withoutKeyTmplRef,
          createArgument = recordFields,
          choiceId = choiceId,
          choiceArgument = Value.ValueUnit,
        ),
      )
    }

    "resolve package name" in {

      val compiledPkgs = ConcurrentCompiledPackages(compilerConfig)
      compiledPkgs.addPackage(defaultPackageId, pkg)
      val preprocessor = new preprocessing.Preprocessor(compiledPkgs)

      val priority = Map(pkgName -> defaultPackageId)

      forEvery(cmdsByPackageName)(cmd =>
        inside(
          preprocessor.commandPreprocessor.unsafePreprocessApiCommands(
            priority,
            ImmArray(cmd),
          )
        ) { case ImmArray(speedyCmd) =>
          val pkgId = speedyCmd match {
            case Command.Create(templateId, _) => templateId.packageId
            case Command.ExerciseTemplate(templateId, _, _, _) => templateId.packageId
            case Command.ExerciseByKey(templateId, _, _, _) => templateId.packageId
            case Command.CreateAndExercise(templateId, _, _, _) => templateId.packageId
            case _ => fail("unexpected speedy command")
          }
          pkgId shouldBe defaultPackageId
        }
      )
    }

    "reject command with unknown package name" in {

      val compiledPkgs = ConcurrentCompiledPackages(compilerConfig)
      compiledPkgs.addPackage(defaultPackageId, pkg)

      val preprocessor =
        new preprocessing.Preprocessor(compiledPkgs)

      forEvery(cmdsByPackageName)(cmd =>
        a[Error.Preprocessing.UnresolvedPackageName] shouldBe thrownBy(
          preprocessor.commandPreprocessor.unsafePreprocessApiCommands(
            Map.empty,
            ImmArray(cmd),
          )
        )
      )
    }

    "preprocessDisclosedContracts" should {

      "reject duplicate disclosed contract IDs" in {
        val preprocessor =
          new preprocessing.Preprocessor(ConcurrentCompiledPackages(compilerConfig))
        val contract1 = buildDisclosedContract(contractId)
        val contract2 =
          buildDisclosedContract(contractId, templateId = withKeyTmplId, keyHash = Some(keyHash))
        val finalResult = preprocessor
          .preprocessDisclosedContracts(ImmArray(contract1, contract2))
          .consume(pkgs = pkgs)

        inside(finalResult) {
          case Left(
                Error.Preprocessing(Error.Preprocessing.DuplicateDisclosedContractId(`contractId`))
              ) =>
            succeed
        }
      }
    }
  }
}

final class PreprocessorSpecHelpers(majorLanguageVersion: LanguageMajorVersion) {

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkgId-"),
      LanguageVersion.defaultOrLatestStable(majorLanguageVersion),
    )

  implicit val defaultPackageId: Ref.PackageId = parserParameters.defaultPackageId

  lazy val pkg =
    p""" metadata ( 'my-nice-package' : '1.0.0' )

        module Mod {

          record @serializable WithoutKey = { owners: List Party, data : Int64 };

          template (this : WithoutKey) = {
            precondition True;
            signatories Mod:WithoutKey {owners} this;
            observers Mod:WithoutKey {owners} this;
            choice Noop (self) (u: Unit) : Unit,
              controllers (Mod:WithoutKey {owners} this),
              observers Nil @Party
              to upure @Unit ();
          };

         record @serializable WithKey = { owners: List Party, data : Int64 };

          template (this : WithKey) = {
            precondition True;
            signatories Mod:WithKey {owners} this;
            observers Mod:WithKey {owners} this;
            choice Noop (self) (u: Unit) : Unit,
              controllers (Mod:WithKey {owners} this),
              observers Nil @Party
              to upure @Unit ();
            key @(List Party) (Mod:WithKey {owners} this) (\ (parties: List Party) -> parties);
          };


        }
    """
  val pkgName = pkg.pkgName
  val pkgs = Map(defaultPackageId -> pkg)
  val alice: Party = Ref.Party.assertFromString("Alice")
  val signatories = immutable.TreeSet(alice)
  val parties: ValueList = ValueList(FrontStack(ValueParty(alice)))
  val testKeyName: String = "test-key"
  val contractId: ContractId =
    Value.ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("test-contract-id"),
      Bytes.assertFromString("deadbeef"),
    )
  val pkgRef: PackageRef.Name = Ref.PackageRef.Name(pkgName)
  val withoutKeyTmplId: Ref.TypeConName = Ref.Identifier.assertFromString("-pkgId-:Mod:WithoutKey")
  val withoutKeyTmplRef: Ref.TypeConRef = Ref.TypeConRef(pkgRef, withoutKeyTmplId.qualifiedName)
  val withKeyTmplId: Ref.TypeConName = Ref.Identifier.assertFromString("-pkgId-:Mod:WithKey")
  val withKeyTmplRef: Ref.TypeConRef = Ref.TypeConRef(pkgRef, withKeyTmplId.qualifiedName)
  val key: Value.ValueRecord = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueText(testKeyName),
      None -> Value.ValueList(FrontStack.from(ImmArray(ValueParty(alice)))),
    ),
  )
  val keyHash: Hash = crypto.Hash.assertHashContractKey(withKeyTmplId, pkgName, key)

  val choiceId: Ref.Name = Ref.Name.assertFromString("Noop")

  def buildDisclosedContract(
      contractId: ContractId = contractId,
      templateId: Ref.TypeConName = withoutKeyTmplId,
      withNormalization: Boolean = true,
      withFieldsReversed: Boolean = false,
      keyHash: Option[Hash] = None,
  ): FatContractInstanceImpl = {
    val recordFields = ImmArray(
      (if (withNormalization) None else Some(Ref.Name.assertFromString("owners"))) -> parties,
      (if (withNormalization) None else Some(Ref.Name.assertFromString("data"))) -> Value
        .ValueInt64(42L),
    )
    FatContractInstanceImpl(
      version = TransactionVersion.StableVersions.max,
      contractId = contractId,
      packageName = pkgName,
      packageVersion = pkg.pkgVersion,
      templateId = templateId,
      createArg = Value.ValueRecord(
        if (withNormalization) None else Some(templateId),
        if (withFieldsReversed) recordFields.reverse else recordFields,
      ),
      signatories = signatories,
      stakeholders = signatories,
      contractKeyWithMaintainers = keyHash.map(_ =>
        GlobalKeyWithMaintainers.assertBuild(templateId, key, signatories, pkgName)
      ),
      createdAt = data.Time.Timestamp.Epoch,
      cantonData = Bytes.Empty,
    )
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Product",
      "org.wartremover.warts.Serializable",
      "org.wartremover.warts.JavaSerializable",
    )
  )
  def acceptDisclosedContract(result: Either[Error, ImmArray[DisclosedContract]]): Assertion = {
    import Inside._
    import Inspectors._
    import Matchers._

    inside(result) { case Right(disclosedContracts) =>
      forAll(disclosedContracts.toList) {
        _.argument match {
          case SValue.SRecord(`withoutKeyTmplId`, fields, values) =>
            fields shouldBe ImmArray(
              Ref.Name.assertFromString("owners"),
              Ref.Name.assertFromString("data"),
            )
            values shouldBe ArrayList(
              SValue.SList(FrontStack(SValue.SParty(alice))),
              SValue.SInt64(42L),
            )

          case _ =>
            fail()
        }
      }
    }
  }
}
