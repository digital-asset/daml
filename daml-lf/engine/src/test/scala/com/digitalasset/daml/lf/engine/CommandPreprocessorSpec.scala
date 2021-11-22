// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command._
import com.daml.lf.data._
import com.daml.lf.transaction.test.TransactionBuilder.newCid
import com.daml.lf.value.Value._
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class CommandPreprocessorSpec
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  import com.daml.lf.testing.parser.Implicits._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  private implicit val defaultPackageId = defaultParserParameters.defaultPackageId

  private[this] val pkg =
    p"""
        module Mod {

          record @serializable Box a = { content: a };

          record @serializable Record = { owners: List Party, data : Int64 };

          template (this : Record) = {
            precondition True;
            signatories Mod:Record {owners} this;
            observers Mod:Record {owners} this;
            agreement "Agreement";
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
            agreement "Agreement";
            choice Change (self) (newCid: ContractId Mod:Record) : ContractId Mod:RecordRef,
                controllers Mod:RecordRef {owners} this,
                observers Nil @Party
              to create @Mod:RecordRef Mod:RecordRef { owners = Mod:RecordRef {owners} this, cid = newCid };
            key @(List Party) (Mod:RecordRef {owners} this) (\ (parties: List Party) -> parties);
          };

        }
    """

  private[this] val compiledPackage = ConcurrentCompiledPackages()
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  private[this] val valueParties = ValueList(FrontStack(ValueParty("Alice")))

  "preprocessCommand" should {

    val defaultPreprocessor = new CommandPreprocessor(compiledPackage.interface, true, false)

    "reject improperly typed commands" in {

      // TEST_EVIDENCE: Input Validation: well formed create command is accepted
      val validCreate = CreateCommand(
        "Mod:Record",
        ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
      )
      // TEST_EVIDENCE: Input Validation: well formed exercise command is accepted
      val validExe = ExerciseCommand(
        "Mod:Record",
        newCid,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Input Validation: well formed exercise-by-key command is accepted
      val validExeByKey = ExerciseByKeyCommand(
        "Mod:Record",
        valueParties,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Input Validation: well formed create-and-exercise command is accepted
      val validCreateAndExe = CreateAndExerciseCommand(
        "Mod:Record",
        ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Input Validation: well formed fetch command is accepted
      val validFetch = FetchCommand(
        "Mod:Record",
        newCid,
      )
      // TEST_EVIDENCE: Input Validation: well formed fetch-by-key command is accepted
      val validFetchByKey = FetchByKeyCommand(
        "Mod:Record",
        valueParties,
      )
      // TEST_EVIDENCE: Input Validation: well formed lookup command is accepted
      val validLookup = LookupByKeyCommand(
        "Mod:Record",
        valueParties,
      )
      val noErrorTestCases = Table[Command](
        "command",
        validCreate,
        validExe,
        validExeByKey,
        validCreateAndExe,
        validFetch,
        validFetchByKey,
        validLookup,
      )

      val errorTestCases = Table[Command, ResultOfATypeInvocation[_]](
        ("command", "error"),
        // TEST_EVIDENCE: Input Validation: ill-formed create command is rejected
        validCreate.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreate.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Input Validation: ill-formed exercise command is rejected
        validExe.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExe.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExe.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Input Validation: ill-formed exercise-by-key command is rejected
        validExeByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        validExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Input Validation: ill-formed create-and-exercise command is rejected
        validCreateAndExe.copy(templateId = "Mod:Undefined") ->
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
        // TEST_EVIDENCE: Input Validation: ill-formed fetch command is rejected
        validFetch.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        // TEST_EVIDENCE: Input Validation: ill-formed fetch-by-key command is rejected
        validFetchByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validFetchByKey.copy(key = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Input Validation: ill-formed lookup command is rejected
        validLookup.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validLookup.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
      )

      forEvery(noErrorTestCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessCommand(command)) shouldBe a[Success[_]]
      }

      forEvery(errorTestCases) { (command, typ) =>
        inside(Try(defaultPreprocessor.unsafePreprocessCommand(command))) {
          case Failure(error: Error.Preprocessing.Error) =>
            error shouldBe typ
        }
      }
    }

    def contractIdTestCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ApiCommand](
      "command",
      CreateCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
      ),
      ExerciseCommand(
        "Mod:RecordRef",
        innocentCid,
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseCommand(
        "Mod:RecordRef",
        culpritCid,
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
        "Change",
        ValueContractId(innocentCid),
      ),
      CreateAndExerciseCommand(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(innocentCid))),
        "Change",
        ValueContractId(culpritCid),
      ),
      ExerciseByKeyCommand(
        "Mod:RecordRef",
        valueParties,
        "Change",
        ValueContractId(culpritCid),
      ),
    )

    "accept all contract IDs when require flags are false" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = false,
        requireV1ContractIdSuffix = false,
      )

      val cids = List(
        ContractId.V1
          .assertBuild(
            crypto.Hash.hashPrivateKey("a suffixed V1 Contract ID"),
            Bytes.assertFromString("00"),
          ),
        ContractId.V1
          .assertBuild(crypto.Hash.hashPrivateKey("a non-suffixed V1 Contract ID"), Bytes.Empty),
        ContractId.V0.assertFromString("#a V0 Contract ID"),
      )

      cids.foreach(cid =>
        forEvery(contractIdTestCases(cids.head, cid))(cmd =>
          Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject V0 Contract IDs when requireV1ContractId flag is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = true,
        requireV1ContractIdSuffix = false,
      )

      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val illegalCid = ContractId.V0.assertFromString("#illegal Contract ID")
      val failure = Failure(Error.Preprocessing.IllegalContractId.V0ContractId(illegalCid))

      forEvery(contractIdTestCases(aLegalCid, anotherLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      )

      forEvery(contractIdTestCases(illegalCid, aLegalCid))(cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      )
    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.interface,
        forbidV0ContractId = true,
        requireV1ContractIdSuffix = true,
      )
      val List(aLegalCid, anotherLegalCid) =
        List("a legal Contract ID", "another legal Contract ID").map(s =>
          ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), Bytes.assertFromString("00"))
        )
      val illegalCid =
        ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey("an illegal Contract ID"), Bytes.Empty)
      val failure = Failure(Error.Preprocessing.IllegalContractId.NonSuffixV1ContractId(illegalCid))

      forEvery(contractIdTestCases(aLegalCid, anotherLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(illegalCid, aLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessCommand(cmd)) shouldBe failure
      }
    }

  }

}
