// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command.ApiCommand
import com.daml.lf.data._
import com.daml.lf.transaction.test.TransactionBuilder.newCid
import com.daml.lf.value.Value._
import org.scalatest.matchers.dsl.ResultOfATypeInvocation
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class ApiCommandPreprocessorSpec
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

          record @serializable MyUnit = {};

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
            agreement "Agreement";
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

  private[this] val compiledPackage = ConcurrentCompiledPackages()
  assert(compiledPackage.addPackage(defaultPackageId, pkg) == ResultDone.Unit)

  private[this] val valueParties = ValueList(FrontStack(ValueParty("Alice")))

  "preprocessCommand" should {

    val defaultPreprocessor =
      new CommandPreprocessor(compiledPackage.pkgInterface, requireV1ContractIdSuffix = false)

    "reject improperly typed ApiCommands" in {

      // TEST_EVIDENCE: Integrity: well formed create API command is accepted
      val validCreate = ApiCommand.Create(
        "Mod:Record",
        ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise API command is accepted
      val validExeTemplate = ApiCommand.Exercise(
        "Mod:Record",
        newCid,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise-by-key API command is accepted
      val validExeByKey = ApiCommand.ExerciseByKey(
        "Mod:Record",
        valueParties,
        "Transfer",
        ValueRecord("", ImmArray("content" -> ValueList(FrontStack(ValueParty("Clara"))))),
      )
      // TEST_EVIDENCE: Integrity: well formed exercise-by-interface command is accepted
      val validExeInterface = ApiCommand.Exercise(
        "Mod:Iface",
        newCid,
        "IfaceChoice",
        ValueUnit,
      )
      // TEST_EVIDENCE: Integrity: well formed create-and-exercise API command is accepted
      val validCreateAndExe = ApiCommand.CreateAndExercise(
        "Mod:Record",
        ValueRecord("", ImmArray("owners" -> valueParties, "data" -> ValueInt64(42))),
        "Transfer",
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
        validCreate.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validCreate.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed exercise API command is rejected
        validExeTemplate.copy(typeId = "Mod:Undefined") ->
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
        validExeByKey.copy(templateId = "Mod:Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(contractKey = ValueList(FrontStack(ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        validExeByKey.copy(choiceId = "Undefined") ->
          a[Error.Preprocessing.Lookup],
        validExeByKey.copy(argument = ValueRecord("", ImmArray("content" -> ValueInt64(42)))) ->
          a[Error.Preprocessing.TypeMismatch],
        // TEST_EVIDENCE: Integrity: ill-formed create-and-exercise API command is rejected
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
      )

      forEvery(noErrorTestCases) { command =>
        Try(defaultPreprocessor.unsafePreprocessApiCommand(command)) shouldBe a[Success[_]]
      }

      forEvery(errorTestCases) { (command, typ) =>
        inside(Try(defaultPreprocessor.unsafePreprocessApiCommand(command))) {
          case Failure(error: Error.Preprocessing.Error) =>
            error shouldBe typ
        }
      }
    }

    def contractIdTestCases(culpritCid: ContractId, innocentCid: ContractId) = Table[ApiCommand](
      "command",
      ApiCommand.Create(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
      ),
      ApiCommand.Exercise(
        "Mod:RecordRef",
        innocentCid,
        "Change",
        ValueContractId(culpritCid),
      ),
      ApiCommand.Exercise(
        "Mod:RecordRef",
        culpritCid,
        "Change",
        ValueContractId(innocentCid),
      ),
      ApiCommand.CreateAndExercise(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(culpritCid))),
        "Change",
        ValueContractId(innocentCid),
      ),
      ApiCommand.CreateAndExercise(
        "Mod:RecordRef",
        ValueRecord("", ImmArray("" -> valueParties, "" -> ValueContractId(innocentCid))),
        "Change",
        ValueContractId(culpritCid),
      ),
      ApiCommand.ExerciseByKey(
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
          Try(cmdPreprocessor.unsafePreprocessApiCommand(cmd)) shouldBe a[Success[_]]
        )
      )

    }

    "reject non suffixed V1 Contract IDs when requireV1ContractIdSuffix is true" in {

      val cmdPreprocessor = new CommandPreprocessor(
        compiledPackage.pkgInterface,
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
        Try(cmdPreprocessor.unsafePreprocessApiCommand(cmd)) shouldBe a[Success[_]]
      }
      forEvery(contractIdTestCases(illegalCid, aLegalCid)) { cmd =>
        Try(cmdPreprocessor.unsafePreprocessApiCommand(cmd)) shouldBe failure
      }
    }

  }

}
