// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.command.{ApiCommand, ReplayCommand}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.speedy.Compiler
import com.digitalasset.daml.lf.transaction._

import scala.util.{Failure, Success, Try}

class ReplayCommandPreprocessorSpecV2 extends ReplayCommandPreprocessorSpec(LanguageMajorVersion.V2)

class ReplayCommandPreprocessorSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.Implicits.{
    defaultPackageId => _,
    _,
  }

  private implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)

  private implicit val pkgId: Ref.PackageId = parserParameters.defaultPackageId

  private[this] val pkg =
    p"""metadata ( 'pkg' : '1.0.0' )

        module Mod {

          record @serializable EmptyView = {};
          record @serializable NoOpI = { controllers: List Party, cid: Option (ContractId Mod:T) };

          interface (this: I) = {
            viewtype Mod:EmptyView;
            choice NoOpI (self) (arg: Mod:NoOpI): Unit,
                controllers Mod:NoOpI {controllers} arg,
                observers Nil @Party
              to upure @Unit ();
          };

          interface (this: J) = {
            viewtype Mod:EmptyView;
            choice NoOpJ (self) (arg: Mod:NoOpI): Unit,
                controllers Mod:NoOpI {controllers} arg,
                observers Nil @Party
              to upure @Unit ();
          };

          record @serializable S = {};

          template (this : S) = {
            precondition True;
            signatories Nil @Party;
            observers Nil @Party;
            choice NoOpS (self) (arg: Mod:NoOpT) : Unit,
                controllers Mod:NoOpT {controllers} arg,
                observers Nil @Party
              to upure @Unit ();
          };

          record @serializable T = { signatories: List Party, cid: Option (ContractId Mod:T) };
          record @serializable NoOpT = { controllers: List Party, cid: Option (ContractId Mod:T) };
          record @serializable Key = { signatories: List Party, cid: Option (ContractId Mod:T) };

          template (this : T) = {
            precondition True;
            signatories Mod:T {signatories} this;
            observers Nil @Party;
            choice NoOpT (self) (arg: Mod:NoOpT) : Unit,
                controllers Mod:NoOpT {controllers} arg,
                observers Nil @Party
              to upure @Unit ();
            implements Mod:I {
              view = Mod:EmptyView {};
            };
            key @Mod:Key
              (Mod:Key { signatories = Mod:T {signatories} this, cid = None @(ContractId Mod:T) })
              (\ (key: Mod:Key) -> Mod:Key {signatories} key);
          };

        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages(
    Compiler.Config.Default(majorLanguageVersion)
  )
  assert(compiledPackage.addPackage(pkgId, pkg) == ResultDone.Unit)

  val parties = collection.immutable.TreeSet[Ref.Party]("Alice")
  private[this] val valueParties = ValueList(parties.iterator.map(ValueParty).to(FrontStack))

  "preprocessCommand" should {

    val suffixedCid = ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
      Bytes.assertFromString("00"),
    )

    val nonSuffixedCid = ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"),
      Bytes.Empty,
    )

    val someSuffixedCid = ValueOptional(Some(ValueContractId(suffixedCid)))
    val someNonSuffixedCid = ValueOptional(Some(ValueContractId(nonSuffixedCid)))

    val TId: Ref.TypeConName = "Mod:T"
    val TName: Ref.TypeConRef = Ref.TypeConRef(Ref.PackageRef.Name(pkg.pkgName), TId.qualifiedName)

    val IId: Ref.TypeConName = "Mod:I"
    val IName: Ref.TypeConRef = Ref.TypeConRef(Ref.PackageRef.Name(pkg.pkgName), IId.qualifiedName)

    val normalizedRecordSome = ValueRecord("", ImmArray("" -> valueParties, "" -> someSuffixedCid))
    val normalizedRecordNone = ValueRecord("", ImmArray("" -> valueParties))
    val nonNormalizedRecordNone = ValueRecord("", ImmArray("" -> valueParties, "" -> ValueNone))
    val nonNormalizedRecordUpgrade =
      ValueRecord("", ImmArray("" -> valueParties, "" -> ValueNone, "" -> ValueNone))
    val illTypedRecord = ValueRecord("", ImmArray("" -> valueParties, "" -> ValueUnit))
    val invalidUpgradeRecord = ValueRecord(
      "",
      ImmArray("" -> valueParties, "" -> ValueNone, "" -> ValueOptional(Some(ValueUnit))),
    )
    val withInvalidCidRecord =
      ValueRecord("", ImmArray("" -> valueParties, "" -> someNonSuffixedCid))

    val defaultPreprocessor =
      new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireV1ContractIdSuffix = true,
      )

    val validApiCreate = ApiCommand.Create(TId, normalizedRecordSome)
    val validApiExe = ApiCommand.Exercise(TId, suffixedCid, "NoOpT", normalizedRecordSome)
    val validApiExeByInterface =
      ApiCommand.Exercise(IId, suffixedCid, "NoOpI", normalizedRecordSome)
    // exercise on key with cid is rejected at runtime
    val validApiExeByKey =
      ApiCommand.ExerciseByKey(TId, normalizedRecordSome, "NoOpT", normalizedRecordSome)
    val validApiCreateAndExe =
      ApiCommand.CreateAndExercise(TId, normalizedRecordSome, "NoOpT", normalizedRecordSome)

    "accept well-formed ApiCommands" in {
      val testCases = Table[ApiCommand](
        "command",
        // TEST_EVIDENCE: Integrity: well formed create Api command is accepted
        validApiCreate,
        validApiCreate.copy(templateRef = TName),
        validApiCreate.copy(argument = normalizedRecordNone),
        validApiCreate.copy(argument = nonNormalizedRecordNone),
        validApiCreate.copy(argument = nonNormalizedRecordUpgrade),
        // TEST_EVIDENCE: Integrity: well formed exercise API command is accepted
        validApiExe,
        validApiExe.copy(typeRef = TName),
        validApiExe.copy(argument = normalizedRecordNone),
        validApiExe.copy(argument = nonNormalizedRecordNone),
        validApiExe.copy(argument = nonNormalizedRecordUpgrade),
        // TEST_EVIDENCE: Integrity: well formed exercise-by-interface API command is accepted
        validApiExeByInterface,
        validApiExeByInterface.copy(typeRef = IName),
        validApiExeByInterface.copy(argument = normalizedRecordNone),
        validApiExeByInterface.copy(argument = nonNormalizedRecordNone),
        validApiExeByInterface.copy(argument = nonNormalizedRecordUpgrade),
        // TEST_EVIDENCE: Integrity: well formed exercise-by-key API command is accepted
        validApiExeByKey,
        validApiExeByKey.copy(templateRef = TName),
        validApiExeByKey.copy(argument = nonNormalizedRecordNone),
        validApiExeByKey.copy(argument = nonNormalizedRecordNone),
        validApiExeByKey.copy(argument = nonNormalizedRecordNone),
        validApiExeByKey.copy(contractKey = normalizedRecordNone),
        validApiExeByKey.copy(contractKey = nonNormalizedRecordNone),
        validApiExeByKey.copy(contractKey = nonNormalizedRecordUpgrade),
        // TEST_EVIDENCE: Integrity: well formed exercise-by-key API command is accepted
        validApiCreateAndExe,
        validApiCreateAndExe.copy(templateRef = TName),
        validApiCreateAndExe.copy(createArgument = normalizedRecordNone),
        validApiCreateAndExe.copy(createArgument = nonNormalizedRecordNone),
        validApiCreateAndExe.copy(createArgument = nonNormalizedRecordUpgrade),
        validApiCreateAndExe.copy(choiceArgument = normalizedRecordNone),
        validApiCreateAndExe.copy(choiceArgument = nonNormalizedRecordNone),
        validApiCreateAndExe.copy(choiceArgument = nonNormalizedRecordUpgrade),
      )

      forEvery(testCases) { command =>
        Try(
          defaultPreprocessor.unsafePreprocessApiCommand(Map(pkg.pkgName -> pkgId), command)
        ) shouldBe a[Success[_]]
      }
    }

    "reject ill-formed API Commands" in {

      val testCases = Table[ApiCommand, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Integrity: ill-formed create API command is rejected
        validApiCreate.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiCreate.copy(templateRef = "Mod:I") ->
          a[Error.Preprocessing.Lookup],
        validApiCreate.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreate.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreate.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed exercise API command is rejected
        validApiExe.copy(typeRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiExe.copy(typeRef = "Mod:S") ->
          a[Error.Preprocessing.Lookup],
        validApiExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiExe.copy(contractId = nonSuffixedCid) ->
          a[Error.Preprocessing.IllegalContractId],
        validApiExe.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExe.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExe.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed exercise-by-interface API command is rejected
        validApiExeByInterface.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiExeByInterface.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByInterface.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByInterface.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed exercise-by-key API command is rejected
        validApiExeByKey.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiExeByKey.copy(templateRef = "Mod:S") ->
          a[Error.Preprocessing.Lookup],
        validApiExeByKey.copy(contractKey = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByKey.copy(contractKey = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByKey.copy(contractKey = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        validApiExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiExeByKey.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByKey.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiExeByKey.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed create-and-exercise API command is rejected
        validApiCreateAndExe.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiCreateAndExe.copy(templateRef = "Mod:S") ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreateAndExe.copy(createArgument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreateAndExe.copy(createArgument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreateAndExe.copy(createArgument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        validApiCreateAndExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validApiCreateAndExe.copy(choiceArgument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreateAndExe.copy(choiceArgument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validApiCreateAndExe.copy(choiceArgument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
      )

      forEvery(testCases) { (command, expectedError) =>
        inside(
          Try(defaultPreprocessor.unsafePreprocessApiCommand(Map(pkg.pkgName -> pkgId), command))
        ) { case Failure(actualError: Error.Preprocessing.Error) =>
          actualError shouldBe expectedError
        }
      }
    }

    val validReplayCreate = ReplayCommand.Create(TId, normalizedRecordSome)
    val validReplayExe =
      ReplayCommand.Exercise(TId, None, suffixedCid, "NoOpT", normalizedRecordSome)
    val validReplayExeByInterface =
      ReplayCommand.Exercise(TId, Some(IId), suffixedCid, "NoOpI", normalizedRecordSome)
    val validReplayExeByKey =
      ReplayCommand.ExerciseByKey(TId, normalizedRecordSome, "NoOpT", normalizedRecordSome)
    val validReplayFetch = ReplayCommand.Fetch(TId, None, suffixedCid)
    val validReplayFetchByKey = ReplayCommand.FetchByKey(TId, normalizedRecordSome)
    val validReplayLookupByKey = ReplayCommand.LookupByKey(TId, normalizedRecordSome)

    "accept well-formed ReplayCommand" in {
      val testCases = Table[ReplayCommand](
        "command",
        // TEST_EVIDENCE: Integrity: well formed create replay command is accepted
        validReplayCreate,
        validReplayCreate.copy(argument = normalizedRecordNone),
        // TEST_EVIDENCE: Integrity: well formed exercise replay command is accepted
        validReplayExe,
        validReplayExe.copy(argument = normalizedRecordNone),
        // TEST_EVIDENCE: Integrity: well formed exercise-by-interface replay command is accepted
        validReplayExeByInterface,
        validReplayExeByInterface.copy(argument = normalizedRecordNone),
        // TEST_EVIDENCE: Integrity: well formed API command is accepted
        validReplayExeByKey,
        validReplayExeByKey.copy(contractKey = normalizedRecordNone),
        validReplayExeByKey.copy(argument = normalizedRecordNone),
        // TEST_EVIDENCE: Integrity: well formed fetch replay command is accepted
        validReplayFetch,
        validReplayFetch.copy(interfaceId = Some(IId)),
        // TEST_EVIDENCE: Integrity: well formed fetch-by-key replay command is accepted
        validReplayFetchByKey,
        validReplayFetchByKey.copy(key = normalizedRecordNone),
        // TEST_EVIDENCE: Integrity: well formed lookup-by-key replay command is accepted
        validReplayLookupByKey,
        validReplayLookupByKey.copy(contractKey = normalizedRecordNone),
      )
      forEvery(testCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessReplayCommand(command)) shouldBe a[Success[_]]
      }
    }

    "reject ill-formed ReplayCommand" in {
      val testCases = Table[ReplayCommand, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Integrity: ill-formed create replay command is rejected
        validReplayCreate.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validReplayCreate.copy(templateId = "Mod:I") ->
          a[Error.Preprocessing.Lookup],
        validReplayCreate.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayCreate.copy(argument = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayCreate.copy(argument = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayCreate.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayCreate.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed exercise replay command is rejected
        validReplayExe.copy(templateId = "Mod:I") ->
          a[Error.Preprocessing.Lookup],
        validReplayExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validReplayExe.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExe.copy(argument = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExe.copy(argument = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExe.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExe.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed exercise-by-interface replay command is rejected
        validReplayExeByInterface.copy(templateId = "Mod:I") ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByInterface.copy(interfaceId = Some("Mod:R")) ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByInterface.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByInterface.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByInterface.copy(argument = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByInterface.copy(argument = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByInterface.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByInterface.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed API command is rejected
        validReplayExeByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByKey.copy(templateId = "Mod:I") ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validReplayExeByKey.copy(contractKey = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByKey.copy(contractKey = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByKey.copy(contractKey = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByKey.copy(contractKey = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByKey.copy(contractKey = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        validReplayExeByKey.copy(argument = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByKey.copy(argument = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByKey.copy(argument = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayExeByKey.copy(argument = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayExeByKey.copy(argument = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed fetch replay command is rejected
        validReplayFetch.copy(templateId = IId) ->
          a[Error.Preprocessing.Lookup],
        validReplayFetch.copy(interfaceId = "Mod:J") ->
          a[Error.Preprocessing.Lookup],
        validReplayFetch.copy(coid = nonSuffixedCid) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed fetch-by-key replay command is rejected
        validReplayFetchByKey.copy(templateId = IId) ->
          a[Error.Preprocessing.Lookup],
        validReplayFetchByKey.copy(key = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayFetchByKey.copy(key = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayFetchByKey.copy(key = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayFetchByKey.copy(key = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayFetchByKey.copy(key = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
        // TEST_EVIDENCE: Integrity: ill-formed lookup-by-key replay command is rejected
        validReplayLookupByKey.copy(templateId = IId) ->
          a[Error.Preprocessing.Lookup],
        validReplayLookupByKey.copy(contractKey = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayLookupByKey.copy(contractKey = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayLookupByKey.copy(contractKey = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validReplayLookupByKey.copy(contractKey = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validReplayLookupByKey.copy(contractKey = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
      )
      forEvery(testCases) { (command, expectedError) =>
        inside(Try(defaultPreprocessor.unsafePreprocessReplayCommand(command))) {
          case Failure(actualError: Error.Preprocessing.Error) =>
            actualError shouldBe expectedError
        }
      }
    }

    def toGlobalKey(record: ValueRecord) =
      Some(GlobalKeyWithMaintainers.assertBuild(TId, record, parties, pkg.pkgName))

    val validDisclosure = FatContractInstanceImpl(
      version = pkg.languageVersion,
      contractId = suffixedCid,
      packageName = pkg.pkgName,
      packageVersion = pkg.pkgVersion,
      templateId = TId,
      createArg = normalizedRecordSome,
      signatories = parties,
      stakeholders = parties,
      contractKeyWithMaintainers = toGlobalKey(normalizedRecordNone),
      createdAt = Time.Timestamp.now(),
      cantonData = Bytes.Empty,
    )

    "accept well-formed disclosure" in {
      val testCases = Table[FatContractInstance](
        "disclose contract",
        // TEST_EVIDENCE: Integrity: disclosure with well-formed argument is accepted
        // other fields of FatDisclosure are checked by model conformance at the beginning of interpretation
        validDisclosure,
        validDisclosure.copy(createArg = normalizedRecordNone),
      )

      forEvery(testCases) { disclosure =>
        val r = Try(defaultPreprocessor.unsafePreprocessDisclosedContract(disclosure))
        r shouldBe a[Success[_]]
      }
    }

    "reject ill-formed disclosure" in {
      val testCases = Table[FatContractInstance, ResultOfATypeInvocation[_]](
        ("disclose contract", "error"),
        // TEST_EVIDENCE: Integrity: disclosure with ill-formed argument is rejected
        // other fields of FatDisclosure are checked by model conformance at the beginning of interpretation
        validDisclosure.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validDisclosure.copy(createArg = illTypedRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validDisclosure.copy(createArg = nonNormalizedRecordNone) ->
          a[Error.Preprocessing.NormalizationError],
        validDisclosure.copy(createArg = nonNormalizedRecordUpgrade) ->
          a[Error.Preprocessing.NormalizationError],
        validDisclosure.copy(createArg = invalidUpgradeRecord) ->
          a[Error.Preprocessing.TypeMismatch],
        validDisclosure.copy(createArg = withInvalidCidRecord) ->
          a[Error.Preprocessing.IllegalContractId],
      )

      forEvery(testCases) { (disclosure, expectedError) =>
        inside(Try(defaultPreprocessor.unsafePreprocessDisclosedContract(disclosure))) {
          case Failure(actualError: Error.Preprocessing.Error) =>
            actualError shouldBe expectedError
        }
      }
    }
  }
}
