// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.command.ApiCommand
import com.daml.lf.data.Ref.{PackageId, Name, QualifiedName, Identifier, Party, TypeConName}
import com.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.daml.lf.language.Ast.Package
import com.daml.lf.speedy.InitialSeeding
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.lf.transaction.{ContractKeyUniquenessMode, GlobalKey, GlobalKeyWithMaintainers}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{
  VersionedContractInstance,
  ContractInstance,
  ValueRecord,
  ValueParty,
  ValueInt64,
  ValueContractId,
  ContractId,
}
import com.daml.logging.LoggingContext
import java.io.File
import org.scalatest.EitherValues
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class ContractKeySpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import ContractKeySpec._

  private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private val (basicTestsPkgId, basicTestsPkg, allPackages) = loadPackage(
    "daml-lf/tests/BasicTests.dar"
  )

  val basicTestsSignatures = language.PackageInterface(Map(basicTestsPkgId -> basicTestsPkg))

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: VersionedContractInstance =
    assertAsVersionedContract(
      ContractInstance(
        TypeConName(basicTestsPkgId, withKeyTemplate),
        ValueRecord(
          Some(BasicTests_WithKey),
          ImmArray(
            (Some[Ref.Name]("p"), ValueParty(alice)),
            (Some[Ref.Name]("k"), ValueInt64(42)),
          ),
        ),
        "",
      )
    )

  val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("BasicTests:Simple:1") ->
        assertAsVersionedContract(
          ContractInstance(
            TypeConName(basicTestsPkgId, "BasicTests:Simple"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
              ImmArray((Some[Name]("p"), ValueParty(party))),
            ),
            "",
          )
        ),
      toContractId("BasicTests:CallablePayout:1") ->
        assertAsVersionedContract(
          ContractInstance(
            TypeConName(basicTestsPkgId, "BasicTests:CallablePayout"),
            ValueRecord(
              Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
              ImmArray(
                (Some[Ref.Name]("giver"), ValueParty(alice)),
                (Some[Ref.Name]("receiver"), ValueParty(bob)),
              ),
            ),
            "",
          )
        ),
      toContractId("BasicTests:WithKey:1") ->
        withKeyContractInst,
    )

  val defaultKey = Map(
    GlobalKey.assertBuild(
      TypeConName(basicTestsPkgId, withKeyTemplate),
      ValueRecord(None, ImmArray((None, ValueParty(alice)), (None, ValueInt64(42)))),
    )
      ->
        toContractId("BasicTests:WithKey:1")
  )

  private[this] def lookupPackage(pkgId: PackageId): Option[Package] = {
    allPackages.get(pkgId)
  }

  private[this] def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
    (key.globalKey.templateId, key.globalKey.key) match {
      case (
            BasicTests_WithKey,
            ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
          ) =>
        Some(toContractId("BasicTests:WithKey:1"))
      case _ =>
        None
    }

  private[this] val suffixLenientEngine = newEngine()
  private[this] val preprocessor =
    new preprocessing.Preprocessor(
      ConcurrentCompiledPackages(suffixLenientEngine.config.getCompilerConfig)
    )

  "contract key" should {
    val now = Time.Timestamp.now()
    val submissionSeed = crypto.Hash.hashPrivateKey("contract key")
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, now)

    // TEST_EVIDENCE: Integrity: contract keys should be evaluated only when executing create
    "be evaluated only when executing create" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyWhenExecutingCreate")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )
      val exerciseArg =
        ValueRecord(
          Some(Identifier(basicTestsPkgId, "BasicTests:DontExecuteCreate")),
          ImmArray.Empty,
        )

      val submitters = Set(alice)

      inside(
        preprocessor
          .preprocessApiCommands(
            ImmArray(
              ApiCommand.CreateAndExercise(templateId, createArg, "DontExecuteCreate", exerciseArg)
            )
          )
          .consume(_ => None, lookupPackage, lookupKey)
      ) { case Right(cmds) =>
        val result = suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = now,
            submissionTime = now,
            seeding = InitialSeeding.TransactionSeed(txSeed),
          )
          .consume(_ => None, lookupPackage, lookupKey)
        result shouldBe a[Right[_, _]]
      }
    }

    // TEST_EVIDENCE: Integrity: contract keys should be evaluated after ensure clause
    "be evaluated after ensure clause" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyAfterEnsureClause")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      inside(
        preprocessor
          .preprocessApiCommands(ImmArray(ApiCommand.Create(templateId, createArg)))
          .consume(_ => None, lookupPackage, lookupKey)
      ) { case Right(cmds) =>
        val result = suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = now,
            submissionTime = now,
            seeding = InitialSeeding.TransactionSeed(txSeed),
          )
          .consume(_ => None, lookupPackage, lookupKey)
        result shouldBe a[Left[_, _]]
        inside(result) { case Left(err) =>
          err.message should not include ("Boom")
          err.message should include("Template precondition violated")
        }
      }
    }

    // TEST_EVIDENCE: Integrity: contract keys must have a non-empty set of maintainers
    "not be create if has an empty set of maintainer" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("sig"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      inside(
        preprocessor
          .preprocessApiCommands(ImmArray(ApiCommand.Create(templateId, createArg)))
          .consume(_ => None, lookupPackage, lookupKey)
      ) { case Right(cmds) =>
        val result = suffixLenientEngine
          .interpretCommands(
            validating = false,
            submitters = submitters,
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = now,
            submissionTime = now,
            seeding = InitialSeeding.TransactionSeed(txSeed),
          )
          .consume(_ => None, lookupPackage, lookupKey)

        inside(result) { case Left(err) =>
          err.message should include(
            "Update failed due to a contract key with an empty sey of maintainers"
          )
        }
      }
    }

    // Note that we provide no stability for multi key semantics so
    // these tests serve only as an indication of the current behavior
    // but can be changed freely.
    "multi keys" should {
      import com.daml.lf.language.{LanguageVersion => LV}
      val nonUckEngine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LV.DevVersions,
          contractKeyUniqueness = ContractKeyUniquenessMode.Off,
          forbidV0ContractId = true,
          requireSuffixedGlobalContractId = true,
        )
      )
      val uckEngine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LV.DevVersions,
          contractKeyUniqueness = ContractKeyUniquenessMode.Strict,
          forbidV0ContractId = true,
          requireSuffixedGlobalContractId = true,
        )
      )
      val (multiKeysPkgId, _, allMultiKeysPkgs) = loadPackage("daml-lf/tests/MultiKeys.dar")
      val lookupPackage = allMultiKeysPkgs.get(_)
      val keyedId = Identifier(multiKeysPkgId, "MultiKeys:Keyed")
      val opsId = Identifier(multiKeysPkgId, "MultiKeys:KeyOperations")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("multikeys")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)

      val cid1 = toContractId("1")
      val cid2 = toContractId("2")
      val keyedInst = assertAsVersionedContract(
        ContractInstance(
          TypeConName(multiKeysPkgId, "MultiKeys:Keyed"),
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          "",
        )
      )
      val contracts = Map(cid1 -> keyedInst, cid2 -> keyedInst)
      val lookupContract = contracts.get(_)
      def lookupKey(key: GlobalKeyWithMaintainers): Option[ContractId] =
        (key.globalKey.templateId, key.globalKey.key) match {
          case (
                `keyedId`,
                ValueParty(`party`),
              ) =>
            Some(cid1)
          case _ =>
            None
        }
      def run(engine: Engine, choice: String, argument: Value) = {
        val cmd = ApiCommand.CreateAndExercise(
          opsId,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          choice,
          argument,
        )
        inside(
          preprocessor
            .preprocessApiCommands(ImmArray(cmd))
            .consume(lookupContract, lookupPackage, lookupKey)
        ) { case Right(cmds) =>
          engine
            .interpretCommands(
              validating = false,
              submitters = Set(party),
              readAs = Set.empty,
              commands = cmds,
              ledgerTime = let,
              submissionTime = let,
              seeding = seeding,
            )
            .consume(lookupContract, lookupPackage, lookupKey)
        }
      }
      val emptyRecord = ValueRecord(None, ImmArray.Empty)
      // The cid returned by a fetchByKey at the beginning
      val keyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid1))))
      // The cid not returned by a fetchByKey at the beginning
      val nonKeyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid2))))
      val twoCids =
        ValueRecord(None, ImmArray((None, ValueContractId(cid1)), (None, ValueContractId(cid2))))
      val createOverwritesLocal = ("CreateOverwritesLocal", emptyRecord)
      val createOverwritesUnknownGlobal = ("CreateOverwritesUnknownGlobal", emptyRecord)
      val createOverwritesKnownGlobal = ("CreateOverwritesKnownGlobal", emptyRecord)
      val fetchDoesNotOverwriteGlobal = ("FetchDoesNotOverwriteGlobal", nonKeyResultCid)
      val fetchDoesNotOverwriteLocal = ("FetchDoesNotOverwriteLocal", keyResultCid)
      val localArchiveOverwritesUnknownGlobal = ("LocalArchiveOverwritesUnknownGlobal", emptyRecord)
      val localArchiveOverwritesKnownGlobal = ("LocalArchiveOverwritesKnownGlobal", emptyRecord)
      val globalArchiveOverwritesUnknownGlobal = ("GlobalArchiveOverwritesUnknownGlobal", twoCids)
      val globalArchiveOverwritesKnownGlobal1 = ("GlobalArchiveOverwritesKnownGlobal1", twoCids)
      val globalArchiveOverwritesKnownGlobal2 = ("GlobalArchiveOverwritesKnownGlobal2", twoCids)
      val rollbackCreateNonRollbackFetchByKey = ("RollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey =
        ("RollbackFetchByKeyRollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyNonRollbackCreate = ("RollbackFetchByKeyNonRollbackCreate", emptyRecord)
      val rollbackFetchNonRollbackCreate = ("RollbackFetchNonRollbackCreate", keyResultCid)
      val rollbackGlobalArchiveNonRollbackCreate =
        ("RollbackGlobalArchiveNonRollbackCreate", keyResultCid)
      val rollbackCreateNonRollbackGlobalArchive =
        ("RollbackCreateNonRollbackGlobalArchive", keyResultCid)
      val rollbackGlobalArchiveUpdates =
        ("RollbackGlobalArchiveUpdates", twoCids)
      val rollbackGlobalArchivedLookup =
        ("RollbackGlobalArchivedLookup", keyResultCid)
      val rollbackGlobalArchivedCreate =
        ("RollbackGlobalArchivedCreate", keyResultCid)

      // regression tests for https://github.com/digital-asset/daml/issues/14171
      val rollbackExerciseCreateFetchByKey =
        ("RollbackExerciseCreateFetchByKey", keyResultCid)
      val rollbackExerciseCreateLookup =
        ("RollbackExerciseCreateLookup", keyResultCid)

      val allCases = Table(
        ("choice", "argument"),
        createOverwritesLocal,
        createOverwritesUnknownGlobal,
        createOverwritesKnownGlobal,
        fetchDoesNotOverwriteGlobal,
        fetchDoesNotOverwriteLocal,
        localArchiveOverwritesUnknownGlobal,
        localArchiveOverwritesKnownGlobal,
        globalArchiveOverwritesUnknownGlobal,
        globalArchiveOverwritesKnownGlobal1,
        globalArchiveOverwritesKnownGlobal2,
        rollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyNonRollbackCreate,
        rollbackFetchNonRollbackCreate,
        rollbackGlobalArchiveNonRollbackCreate,
        rollbackCreateNonRollbackGlobalArchive,
        rollbackGlobalArchiveUpdates,
        rollbackGlobalArchivedLookup,
        rollbackGlobalArchivedCreate,
        rollbackExerciseCreateFetchByKey,
        rollbackExerciseCreateLookup,
      )

      val nonUckFailures = Set(
        "RollbackExerciseCreateLookup",
        "RollbackExerciseCreateFetchByKey",
      )

      val uckFailures = Set(
        "CreateOverwritesKnownGlobal",
        "CreateOverwritesLocal",
        "FetchDoesNotOverwriteGlobal",
        "FetchDoesNotOverwriteLocal",
        "GlobalArchiveOverwritesKnownGlobal1",
        "GlobalArchiveOverwritesKnownGlobal2",
        "GlobalArchiveOverwritesUnknownGlobal",
        "LocalArchiveOverwritesKnownGlobal",
        "RollbackCreateNonRollbackFetchByKey",
        "RollbackCreateNonRollbackGlobalArchive",
        "RollbackFetchByKeyNonRollbackCreate",
        "RollbackFetchByKeyRollbackCreateNonRollbackFetchByKey",
        "RollbackFetchNonRollbackCreate",
        "RollbackGlobalArchiveNonRollbackCreate",
        "RollbackGlobalArchiveUpdates",
      )

      // TEST_EVIDENCE: Integrity: contract key behaviour (non-unique mode)
      "non-uck mode" in {
        forEvery(allCases) { case (name, arg) =>
          if (nonUckFailures.contains(name)) {
            run(nonUckEngine, name, arg) shouldBe a[Left[_, _]]
          } else {
            run(nonUckEngine, name, arg) shouldBe a[Right[_, _]]
          }
        }
      }
      // TEST_EVIDENCE: Integrity: contract key behaviour (unique mode)
      "uck mode" in {
        forEvery(allCases) { case (name, arg) =>
          if (uckFailures.contains(name)) {
            run(uckEngine, name, arg) shouldBe a[Left[_, _]]
          } else {
            run(uckEngine, name, arg) shouldBe a[Right[_, _]]
          }
        }
      }
    }
  }
}

object ContractKeySpec {

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

  private val party = Party.assertFromString("Party")
  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")

  private def newEngine(requireCidSuffixes: Boolean = false) =
    new Engine(
      EngineConfig(
        allowedLanguageVersions = language.LanguageVersion.DevVersions,
        forbidV0ContractId = true,
        requireSuffixedGlobalContractId = requireCidSuffixes,
      )
    )

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

}
