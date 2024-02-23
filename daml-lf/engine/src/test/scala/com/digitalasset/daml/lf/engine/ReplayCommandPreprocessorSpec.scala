// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command.ReplayCommand
import com.daml.lf.data._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.transaction.test.TransactionBuilder.newCid
import com.daml.lf.value.Value._
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.speedy.Compiler

import scala.util.{Failure, Success, Try}

class ReplayCommandPreprocessorSpecV2 extends ReplayCommandPreprocessorSpec(LanguageMajorVersion.V2)

class ReplayCommandPreprocessorSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import com.daml.lf.testing.parser.Implicits.SyntaxHelper
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  private implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor(majorLanguageVersion)

  private implicit val defaultPackageId: Ref.PackageId = parserParameters.defaultPackageId

  private[this] val pkg =
    p"""metadata ( 'pkg' : '1.0.0' )

        module Mod {

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
        requireV1ContractIdSuffix = false,
        enableContractUpgrading = false,
      )

    "reject improperly typed ApiCommands" in {

      // TEST_EVIDENCE: Integrity: well formed create replay command is accepted
      val validCreate = ReplayCommand.Create(
        "Mod:Record",
        ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise replay command is accepted
      val validExe = ReplayCommand.Exercise(
        "Mod:Record",
        "",
        newCid,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise-by-key command is accepted
      val validExeByKey = ReplayCommand.ExerciseByKey(
        "Mod:Record",
        valueParties,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      val noErrorTestCases = Table[ReplayCommand](
        "command",
        validCreate,
        validExe,
        validExeByKey,
      )

      val errorTestCases = Table[ReplayCommand, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Integrity: ill-formed create replay command is rejected
        validCreate.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreate.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed exercise replay command is rejected
        validExe.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExe.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed exercise-by-key replay command is rejected
        validExeByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        validExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
      )

      forEvery(noErrorTestCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessReplayCommand(command)) shouldBe a[Success[_]]
      }

      forEvery(errorTestCases) { (command, typ) =>
        inside(Try(defaultPreprocessor.unsafePreprocessReplayCommand(command))) {
          case Failure(error: Error.Preprocessing.Error) =>
            error shouldBe typ
        }
      }
    }

    "reject improperly typed ReplayCommands" in {
      // TEST_EVIDENCE: Integrity: well formed fetch replay command is accepted
      val validFetch = ReplayCommand.Fetch(
        "Mod:Record",
        newCid,
      )
      // TEST_EVIDENCE: Integrity: well formed fetch-by-key replay command is accepted
      val validFetchByKey = ReplayCommand.FetchByKey(
        "Mod:Record",
        valueParties,
      )
      // TEST_EVIDENCE: Integrity: well formed lookup replay command is accepted
      val validLookup = ReplayCommand.LookupByKey(
        "Mod:Record",
        valueParties,
      )
      val noErrorTestCases = Table[ReplayCommand](
        "command",
        validFetch,
        validFetchByKey,
        validLookup,
      )
      val errorTestCases = Table[ReplayCommand, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Integrity: ill-formed fetch command is rejected
        validFetch.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        // TEST_EVIDENCE: Integrity: ill-formed fetch-by-key command is rejected
        validFetchByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validFetchByKey.copy(key = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed lookup command is rejected
        validLookup.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validLookup.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
      )

      forEvery(noErrorTestCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessReplayCommand(command)) shouldBe a[Success[_]]
      }

      forEvery(errorTestCases) { (command, typ) =>
        inside(Try(defaultPreprocessor.unsafePreprocessReplayCommand(command))) {
          case Failure(error: Error.Preprocessing.Error) =>
            error shouldBe typ
        }
      }
    }

    def contractIdTestCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ReplayCommand](
      "command",
      ReplayCommand.Create(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
      ),
      ReplayCommand.Exercise(
        "Mod:RecordRef",
        "",
        innocentCid,
        "Change",
        ValueContractId(culpritCid),
      ),
      ReplayCommand.Exercise(
        "Mod:RecordRef",
        "",
        culpritCid,
        "Change",
        ValueContractId(innocentCid),
      ),
      ReplayCommand.ExerciseByKey(
        "Mod:RecordRef",
        valueParties,
        "Change",
        ValueContractId(culpritCid),
      ),
    )

    "accept all contract IDs when require flags are false" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireV1ContractIdSuffix = false,
        enableContractUpgrading = false,
      )

      val cids = List(
        ContractId.V1
          .assertBuild(
            crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
            Bytes.assertFromString("00"),
          ),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
      )

      cids.foreach(cid =>
        forEvery(contractIdTestCases(cids.head, cid))(cmd =>
          Try(cmdPreprocessor.unsafePreprocessReplayCommand(cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.pkgInterface,
        requireV1ContractIdSuffix = true,
        enableContractUpgrading = false,
      )
      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCid))

      forEvery(contractIdTestCases(aLegalCid, anotherLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessReplayCommand(cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(illegalCid, aLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessReplayCommand(cmd)) shouldBe failure
      }
    }

  }

}
