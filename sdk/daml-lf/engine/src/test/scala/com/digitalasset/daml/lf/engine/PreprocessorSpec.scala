// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.nameof.NameOf.qualifiedNameOfMember
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, PackageRef, PackageVersion, Party}
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.command.{ApiCommand, ApiContractKey}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion, LookupError}
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
  CreationTime,
  FatContractInstanceImpl,
  GlobalKey,
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
      val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)
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
      val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)
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
      val preprocessor = preprocessing.Preprocessor.forTesting(compiledPkgs)

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

      val preprocessor = preprocessing.Preprocessor.forTesting(compiledPkgs)

      forEvery(cmdsByPackageName)(cmd =>
        a[Error.Preprocessing.UnresolvedPackageName] shouldBe thrownBy(
          preprocessor.commandPreprocessor.unsafePreprocessApiCommands(
            Map.empty,
            ImmArray(cmd),
          )
        )
      )
    }

    def testBuildPackageResolution(
        packageMap: Seq[(String, String, String)],
        packagePreferenceSet: Seq[String],
        expected: Result[Map[String, String]],
    ): Assertion = {
      val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)

      val result: Result[Map[PackageName, PackageId]] = preprocessor.buildPackageResolution(
        packageMap = packageMap.map { case (pkgId, pkgName, pkgVersion) =>
          PackageId.assertFromString(pkgId) -> (PackageName.assertFromString(
            pkgName
          ) -> PackageVersion.assertFromString(pkgVersion))
        }.toMap,
        packagePreference = packagePreferenceSet.map(Ref.PackageId.assertFromString).toSet,
      )

      result shouldBe expected.map(_.map { case (rawPkgName, rawPkgId) =>
        PackageName.assertFromString(rawPkgName) -> PackageId.assertFromString(rawPkgId)
      })
    }

    "build package resolution map" when {
      "provided with consistent package-map and preference set inputs" in {
        testBuildPackageResolution(
          packageMap = Seq(
            ("pkgId1", "pkgName1", "1.0.0"),
            ("pkgId2", "pkgName1", "1.1.0"),
            ("pkgId3", "pkgName2", "1.0.0"),
          ),
          packagePreferenceSet = Seq("pkgId2", "pkgId3"),
          expected = ResultDone(Map("pkgName1" -> "pkgId2", "pkgName2" -> "pkgId3")),
        )
      }
    }

    "return correct error message" when {
      "provided with a package-id that does not have a package-name counterpart in the package map" in {
        testBuildPackageResolution(
          packageMap = Seq(),
          packagePreferenceSet = Seq("pkgId"),
          expected = ResultError(
            Error.Preprocessing.Lookup(
              LookupError.MissingPackage(Ref.PackageId.assertFromString("pkgId"))
            )
          ),
        )

        testBuildPackageResolution(
          packageMap = Seq(("otherPkgId", "pkgName1", "1.0.0")),
          packagePreferenceSet = Seq("pkgId"),
          expected = ResultError(
            Error.Preprocessing.Lookup(
              LookupError.MissingPackage(Ref.PackageId.assertFromString("pkgId"))
            )
          ),
        )
      }

      "provided with multiple package-ids referencing the same package-name" in {
        testBuildPackageResolution(
          packageMap = Seq(
            ("pkgId1", "pkgName1", "1.0.0"),
            ("pkgId2", "pkgName1", "1.1.0"),
            ("pkgId3", "pkgName2", "1.0.0"),
          ),
          packagePreferenceSet = Seq("pkgId1", "pkgId2", "pkgId3"),
          expected = ResultError(
            Error.Preprocessing.Internal(
              qualifiedNameOfMember[preprocessing.Preprocessor](_.buildPackageResolution()),
              "package pkgId1 and pkgId2 have the same name pkgName1",
              None,
            )
          ),
        )
      }
    }

    "preprocessDisclosedContracts" should {

      "reject duplicate disclosed contract IDs" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)
        val contract1 = buildDisclosedContract(contractId)
        val contract2 =
          buildDisclosedContract(contractId, templateId = withKeyTmplId, key = Some(helpers.key))
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

      "return the sets of disclosed contract IDs and disclosed contract key hashes" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)
        val contract1 =
          buildDisclosedContract(contractId, templateId = withKeyTmplId, key = Some(helpers.key))
        val contractId2 =
          Value.ContractId.V1.assertBuild(
            crypto.Hash.hashPrivateKey("another-contract-id"),
            Bytes.assertFromString("cafe"),
          )

        val contract2 =
          buildDisclosedContract(
            contractId2,
            templateId = withKeyTmplId,
            key = Some(keyBob),
          )
        val finalResult = preprocessor
          .preprocessDisclosedContracts(ImmArray(contract1, contract2))
          .consume(pkgs = pkgs)

        inside(finalResult) { case Right((_, contractIds, keyHashes)) =>
          contractIds shouldBe Set(contract1.contractId, contract2.contractId)
          keyHashes shouldBe Set(keyHash, keyBobHash)
        }
      }
    }

    "prefetchContractIdsAndKeys" should {
      val priority = Map(pkgName -> defaultPackageId)
      val bob: Party = Ref.Party.assertFromString("Bob")
      val bobKey = ValueList(FrontStack(ValueParty(bob)))

      val globalKey1 = GlobalKey.assertBuild(withKeyTmplId, parties, pkgName)
      val globalKey2 = GlobalKey.assertBuild(withKeyTmplId, bobKey, pkgName)

      "extract the keys from ExerciseByKey commands" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)

        val commands = {
          val recordFields = Value.ValueRecord(
            None,
            ImmArray(
              None -> parties,
              None -> Value.ValueInt64(42L),
            ),
          )
          ImmArray(
            ApiCommand.Create(
              templateRef = withKeyTmplRef,
              argument = recordFields,
            ),
            ApiCommand.Exercise(
              typeRef = withKeyTmplRef,
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
            ApiCommand.ExerciseByKey(
              templateRef = withKeyTmplId.toRef,
              contractKey = bobKey,
              choiceId = choiceId,
              argument = Value.ValueUnit,
            ),
          )
        }

        val Right(preprocessedCommands) = preprocessor
          .preprocessApiCommands(priority, commands)
          .consume(pkgs = pkgs)

        val resultAllPrefetch =
          preprocessor.prefetchContractIdsAndKeys(
            preprocessedCommands,
            Seq.empty,
            Set.empty,
            Set.empty,
          )
        inside(resultAllPrefetch) { case ResultPrefetch(contractIds, keys, resume) =>
          contractIds shouldBe Seq(contractId)
          keys shouldBe Seq(globalKey1, globalKey2)
          resume() shouldBe ResultDone.Unit
        }

        val resultAllKeysDisclosed =
          preprocessor.prefetchContractIdsAndKeys(
            preprocessedCommands,
            Seq.empty,
            Set.empty,
            Set(globalKey1.hash, globalKey2.hash),
          )
        inside(resultAllKeysDisclosed) { case ResultPrefetch(contractIds, keys, resume) =>
          contractIds shouldBe Seq(contractId)
          keys shouldBe Seq.empty
          resume() shouldBe ResultDone.Unit
        }

        val resultAllDisclosed =
          preprocessor.prefetchContractIdsAndKeys(
            preprocessedCommands,
            Seq.empty,
            Set(contractId),
            Set(globalKey1.hash, globalKey2.hash),
          )
        resultAllDisclosed shouldBe ResultDone.Unit
      }

      "include explicitly specified keys" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)

        val prefetch = Seq(
          ApiContractKey(withKeyTmplRef, parties),
          ApiContractKey(withKeyTmplRef, bobKey),
        )

        val Right(globalKeys) =
          preprocessor.preprocessApiContractKeys(priority, prefetch).consume(pkgs = pkgs)
        globalKeys shouldBe Seq(globalKey1, globalKey2)

        val resultAllUndisclosed =
          preprocessor.prefetchContractIdsAndKeys(ImmArray.empty, globalKeys, Set.empty, Set.empty)
        inside(resultAllUndisclosed) { case ResultPrefetch(contractIds, keys, resume) =>
          contractIds shouldBe Seq.empty
          keys shouldBe Seq(globalKey1, globalKey2)
          resume() shouldBe ResultDone.Unit
        }

        val resultAllDisclosed =
          preprocessor.prefetchContractIdsAndKeys(
            ImmArray.empty,
            globalKeys,
            Set.empty,
            globalKeys.map(_.hash).toSet,
          )
        resultAllDisclosed shouldBe ResultDone.Unit
      }

      "extract contract IDs from commands" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)

        val moreContractIds: Seq[ContractId] = (1 to 5).map(i =>
          Value.ContractId.V1.assertBuild(
            crypto.Hash.hashPrivateKey(s"test-contract-id$i"),
            Bytes.assertFromString("deadbeef"),
          )
        )

        val commands = {
          def createArg(ids: Seq[ContractId]) = Value.ValueRecord(
            None,
            ImmArray(
              None -> parties,
              None -> Value.ValueList(FrontStack.from(ids.map(Value.ValueContractId))),
            ),
          )
          def choiceArg(ids: Seq[ContractId]) =
            Value.ValueList(FrontStack.from(ids.map(Value.ValueContractId)))
          ImmArray(
            ApiCommand.Create(
              templateRef = withContractIdTmplRef,
              argument = createArg(Seq(contractId)),
            ),
            ApiCommand.Exercise(
              typeRef = withContractIdTmplRef,
              contractId = contractId,
              choiceId = choiceId,
              argument = choiceArg(moreContractIds.slice(0, 1)),
            ),
            ApiCommand.ExerciseByKey(
              templateRef = withContractIdTmplRef,
              contractKey = parties,
              choiceId = choiceId,
              argument = choiceArg(moreContractIds.slice(1, 3)),
            ),
            ApiCommand.CreateAndExercise(
              templateRef = withContractIdTmplRef,
              createArgument = createArg(moreContractIds.slice(3, 4)),
              choiceId = choiceId,
              choiceArgument = choiceArg(moreContractIds.slice(4, 5)),
            ),
          )
        }

        val Right(preprocessedCommands) = preprocessor
          .preprocessApiCommands(priority, commands)
          .consume(pkgs = pkgs)

        val resultAllPrefetch =
          preprocessor.prefetchContractIdsAndKeys(
            preprocessedCommands,
            Seq.empty,
            Set.empty,
            Set.empty,
          )
        inside(resultAllPrefetch) { case ResultPrefetch(contractIds, keys, resume) =>
          contractIds.toSet shouldBe ((contractId +: moreContractIds).toSet)
          keys shouldBe Seq(GlobalKey.assertBuild(withContractIdTmplId, parties, pkgName))
          resume() shouldBe ResultDone.Unit
        }

      }

      "fail on contract IDs in keys" in {
        val preprocessor = preprocessing.Preprocessor.forTesting(compilerConfig)

        val commands = ImmArray(
          ApiCommand.ExerciseByKey(
            templateRef = withKeyTmplRef,
            contractKey = parties,
            choiceId = choiceId,
            argument = Value.ValueUnit,
          )
        )

        val Right(preprocessedCommands) = preprocessor
          .preprocessApiCommands(
            priority,
            commands,
          )
          .consume(pkgs = pkgs)

        val commandsWithContractIdAsKey = preprocessedCommands.map {
          case ex: speedy.Command.ExerciseByKey =>
            ex.copy(contractKey = speedy.SValue.SContractId(contractId))
          case other => other
        }

        val result = preprocessor.prefetchContractIdsAndKeys(
          commandsWithContractIdAsKey,
          Seq.empty,
          Set.empty,
          Set.empty,
        )
        inside(result) {
          case ResultError(
                Error.Preprocessing(Error.Preprocessing.ContractIdInContractKey(value))
              ) =>
            value shouldBe Value.ValueContractId(contractId)
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

          record @serializable WithContractId = { owners: List Party, data : List (ContractId Unit) };

          template (this: WithContractId) = {
            precondition True;
            signatories Mod:WithContractId {owners} this;
            observers Mod:WithContractId {owners} this;
            choice Noop (self) (arg: List (ContractId Unit)) : Unit,
              controllers (Mod:WithContractId {owners} this),
              observers Nil @Party
              to upure @Unit ();
            key @(List Party) (Mod:WithContractId {owners} this) (\ (parties: List Party) -> parties);
          };
        }
    """
  val pkgName = pkg.pkgName
  val pkgs = Map(defaultPackageId -> pkg)
  val alice: Party = Ref.Party.assertFromString("Alice")
  val bob: Party = Ref.Party.assertFromString("Bob")
  val signatories = immutable.TreeSet(alice)
  val parties: ValueList = ValueList(FrontStack(ValueParty(alice)))
  val testKeyName: String = "test-key"
  val contractId: ContractId =
    Value.ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("test-contract-id"),
      Bytes.assertFromString("deadbeef"),
    )
  val pkgRef: PackageRef.Name = Ref.PackageRef.Name(pkgName)
  val withoutKeyTmplId: Ref.TypeConId = Ref.Identifier.assertFromString("-pkgId-:Mod:WithoutKey")
  val withoutKeyTmplRef: Ref.TypeConRef = Ref.TypeConRef(pkgRef, withoutKeyTmplId.qualifiedName)
  val withKeyTmplId: Ref.TypeConId = Ref.Identifier.assertFromString("-pkgId-:Mod:WithKey")
  val withKeyTmplRef: Ref.TypeConRef = Ref.TypeConRef(pkgRef, withKeyTmplId.qualifiedName)
  val key: Value.ValueRecord = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueText(testKeyName),
      None -> Value.ValueList(FrontStack.from(ImmArray(ValueParty(alice)))),
    ),
  )
  val keyHash: Hash = crypto.Hash.assertHashContractKey(withKeyTmplId, pkgName, key)

  val withContractIdTmplId: Ref.TypeConId =
    Ref.Identifier.assertFromString("-pkgId-:Mod:WithContractId")
  val withContractIdTmplRef: Ref.TypeConRef =
    Ref.TypeConRef(pkgRef, withContractIdTmplId.qualifiedName)

  val keyBob: Value.ValueRecord = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueText(testKeyName),
      None -> Value.ValueList(FrontStack.from(ImmArray(ValueParty(bob)))),
    ),
  )
  val keyBobHash = crypto.Hash.assertHashContractKey(withKeyTmplId, pkgName, keyBob)

  val choiceId: Ref.Name = Ref.Name.assertFromString("Noop")

  def buildDisclosedContract(
      contractId: ContractId = contractId,
      templateId: Ref.TypeConId = withoutKeyTmplId,
      withNormalization: Boolean = true,
      withFieldsReversed: Boolean = false,
      key: Option[Value] = None,
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
      templateId = templateId,
      createArg = Value.ValueRecord(
        if (withNormalization) None else Some(templateId),
        if (withFieldsReversed) recordFields.reverse else recordFields,
      ),
      signatories = signatories,
      stakeholders = signatories,
      contractKeyWithMaintainers =
        key.map(k => GlobalKeyWithMaintainers.assertBuild(templateId, k, signatories, pkgName)),
      createdAt = CreationTime.CreatedAt(data.Time.Timestamp.Epoch),
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
  def acceptDisclosedContract(
      result: Either[Error, (ImmArray[DisclosedContract], Set[Hash])]
  ): Assertion = {
    import Inside._
    import Inspectors._
    import Matchers._

    inside(result) { case Right((disclosedContracts, keyHashes)) =>
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
      keyHashes shouldBe Set.empty
    }
  }
}
