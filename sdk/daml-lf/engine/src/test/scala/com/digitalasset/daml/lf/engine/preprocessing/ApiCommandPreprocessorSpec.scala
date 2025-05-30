// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder.newCid
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.digitalasset.daml.lf.speedy.Compiler

import scala.util.{Failure, Success, Try}

class ApiCommandPreprocessorSpecV2 extends ApiCommandPreprocessorSpec(LanguageMajorVersion.V2)

class ApiCommandPreprocessorSpec(majorLanguageVersion: LanguageMajorVersion)
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

  private implicit val defaultPackageId: PackageId = parserParameters.defaultPackageId

  private[this] val pkg =
    p"""metadata ( 'pkg' : '1.0.0' )

        module Mod {

          record @serializable MyUnit = {};

          record @serializable Box a = { content: a };

          record @serializable Record = { owners: List Party, data : Int64 };

          template (this : Record) = {
            precondition True;
            signatories Mod:Record {owners} this;
            observers Mod:Record {owners} this;
            choice Transfer (self) (box: Mod:Box (List Party)) : ContractId Mod:Record,
                controllers Mod:Record {owners} this,
                observers Nil @Party
              to create @Mod:Record Mod:Record { owners = Mod:Box @(List Party) {content} box, data = Mod:Record {data} this } ;
            implements Mod:Iface{
              view = Mod:MyUnit {};
              method getCtrls = Mod:Record {owners} this;
            };
            implements Mod:Iface3{
              view = Mod:MyUnit {};
              method getCtrls = Mod:Record {owners} this;
            };
            key @(List Party) (Mod:Record {owners} this) (\ (parties: List Party) -> parties);
          };

          record @serializable RecordRef = { owners: List Party, cid: (ContractId Mod:Record) };

          template (this : RecordRef) = {
            precondition True;
            signatories Mod:RecordRef {owners} this;
            observers Mod:RecordRef {owners} this;
            choice Change (self) (newCid: ContractId Mod:Record) : ContractId Mod:RecordRef,
                controllers Mod:RecordRef {owners} this,
                observers Nil @Party
              to create @Mod:RecordRef Mod:RecordRef { owners = Mod:RecordRef {owners} this, cid = newCid };
            key @(List Party) (Mod:RecordRef {owners} this) (\ (parties: List Party) -> parties);
          };

          interface (this: Iface) = {
            viewtype Mod:MyUnit;
            requires Mod:Iface3;
            method getCtrls: List Party;
            choice IfaceChoice (self) (u:Unit) : Unit
              , controllers (call_method @Mod:Iface getCtrls this)
              to upure @Unit ();
          } ;

          interface (this: Iface2) = {
            viewtype Mod:MyUnit;
            method getCtrls: List Party;
            choice IfaceChoice2 (self) (u:Unit) : Unit
              , controllers (call_method @Mod:Iface2 getCtrls this)
              to upure @Unit ();
          } ;

          interface (this: Iface3) = {
            viewtype Mod:MyUnit;
            method getCtrls: List Party;
            choice IfaceChoice3 (self) (u:Unit) : Unit
              , controllers (call_method @Mod:Iface3 getCtrls this)
              to upure @Unit ();
          } ;

        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages(
    Compiler.Config.Default(majorLanguageVersion)
  )
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  private[this] val valueParties = ValueList(FrontStack(ValueParty("Alice")))

  "preprocessCommand" should {

    val defaultPreprocessor =
      new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireContractIdSuffix = false,
      )

    "reject improperly typed ApiCommands" in {

      // TEST_EVIDENCE: Integrity: well formed create API command is accepted
      val validCreate = ApiCommand.Create(
        templateRef = "Mod:Record",
        argument = ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise API command is accepted
      val validExeTemplate = ApiCommand.Exercise(
        typeRef = "Mod:Record",
        contractId = newCid,
        choiceId = "Transfer",
        argument =
          ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise-by-key API command is accepted
      val validExeByKey = ApiCommand.ExerciseByKey(
        templateRef = "Mod:Record",
        contractKey = valueParties,
        choiceId = "Transfer",
        argument =
          ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise-by-interface command is accepted
      val validExeInterface = ApiCommand.Exercise(
        typeRef = "Mod:Iface",
        contractId = newCid,
        choiceId = "IfaceChoice",
        argument = ValueUnit,
      )
      // TEST_EVIDENCE: Integrity: well formed create-and-exercise API command is accepted
      val validCreateAndExe = ApiCommand.CreateAndExercise(
        templateRef = "Mod:Record",
        createArgument =
          ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
        choiceId = "Transfer",
        choiceArgument =
          ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      val noErrorTestCases = Table[ApiCommand](
        "command",
        validCreate,
        validExeTemplate,
        validExeInterface,
        validExeByKey,
        validCreateAndExe,
      )

      val errorTestCases = Table[ApiCommand, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Integrity: ill-formed create API command is rejected
        validCreate.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreate.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed exercise API command is rejected
        validExeTemplate.copy(typeRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeTemplate.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeTemplate.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: exercise-by-interface command is rejected for a
        // choice of another interface.
        validExeInterface.copy(choiceId = "IfaceChoice2") ->
          a[Error.Preprocessing.Lookup],
        // TEST_EVIDENCE: Integrity: ill-formed exercise-by-key API command is rejected
        validExeByKey.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        validExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed create-and-exercise API command is rejected
        validCreateAndExe.copy(templateRef = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreateAndExe.copy(createArgument =
          ValueRecord("", ImmArray("content" -> ValueInt64(42)))
        ) ->
          a[Error.Preprocessing.TypeMismatch],
        validCreateAndExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreateAndExe.copy(choiceArgument =
          ValueRecord("", ImmArray("content" -> ValueInt64(42)))
        ) ->
          a[Error.Preprocessing.TypeMismatch],
      )

      forEvery(noErrorTestCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessApiCommand(Map.empty, command)) shouldBe a[Success[
          _
        ]]
      }

      forEvery(errorTestCases) { (command, typ) =>
        inside(Try(defaultPreprocessor.unsafePreprocessApiCommand(Map.empty, command))) {
          case Failure(error: Error.Preprocessing.Error) =>
            error shouldBe typ
        }
      }
    }

    def contractIdTestCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ApiCommand](
      "command",
      ApiCommand.Create(
        templateRef = "Mod:RecordRef",
        argument = ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
      ),
      ApiCommand.Exercise(
        typeRef = "Mod:RecordRef",
        contractId = innocentCid,
        choiceId = "Change",
        argument = ValueContractId(culpritCid),
      ),
      ApiCommand.Exercise(
        typeRef = "Mod:RecordRef",
        contractId = culpritCid,
        choiceId = "Change",
        argument = ValueContractId(innocentCid),
      ),
      ApiCommand.CreateAndExercise(
        templateRef = "Mod:RecordRef",
        createArgument =
          ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
        choiceId = "Change",
        choiceArgument = ValueContractId(innocentCid),
      ),
      ApiCommand.CreateAndExercise(
        templateRef = "Mod:RecordRef",
        createArgument =
          ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(innocentCid))),
        choiceId = "Change",
        choiceArgument = ValueContractId(culpritCid),
      ),
      ApiCommand.ExerciseByKey(
        templateRef = "Mod:RecordRef",
        contractKey = valueParties,
        choiceId = "Change",
        argument = ValueContractId(culpritCid),
      ),
    )

    "accept all contract IDs when require flags are false" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireContractIdSuffix = false,
      )

      val unsuffixedCidV1 = ContractId.V1
        .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty)
      val suffixedCidV1 = ContractId.V1.assertBuild(
        crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
        Bytes.assertFromString("00"),
      )
      val unsuffixedCidV2 = ContractId.V2.unsuffixed(
        Time.Timestamp.Epoch,
        crypto.Hash.hashPrivateKey("an unsuffixed V2 Contract ID"),
      )
      val suffixedCidV2 =
        ContractId.V2.assertBuild(unsuffixedCidV2.local, Bytes.assertFromString("00"))
      val cids = List(suffixedCidV1, unsuffixedCidV1, suffixedCidV2, unsuffixedCidV2)

      cids.foreach(cid =>
        forEvery(contractIdTestCases(cids.head, cid))(cmd =>
          Try(cmdPreprocessor.unsafePreprocessApiCommand(Map.empty, cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject non suffixed Contract IDs when requireContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireContractIdSuffix = true,
      )
      val List(aLegalCidV1, anotherLegalCidV1) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val aLegalCidV2 = ContractId.V2.assertBuild(
        Bytes.fromByteArray(Array.fill[Byte](ContractId.V2.localSize)(0x12.toByte)),
        Bytes.assertFromString("00"),
      )
      val illegalCidV1 =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val illegalCidV2 =
        ContractId.V2.unsuffixed(
          Time.Timestamp.Epoch,
          crypto.Hash.hashPrivateKey("an illegal Contract ID"),
        )
      val failureV1 =
        Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCidV1))
      val failureV2 =
        Failure(Error.Preprocessing.IllegalContractId.NonSuffixV2ContractId(illegalCidV2))

      forEvery(contractIdTestCases(aLegalCidV1, anotherLegalCidV1)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessApiCommand(Map.empty, cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(aLegalCidV1, aLegalCidV2)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessApiCommand(Map.empty, cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(illegalCidV1, aLegalCidV1)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessApiCommand(Map.empty, cmd)) shouldBe failureV1
      }
      forEvery(contractIdTestCases(illegalCidV2, aLegalCidV2)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessApiCommand(Map.empty, cmd)) shouldBe failureV2
      }
    }

  }

}
