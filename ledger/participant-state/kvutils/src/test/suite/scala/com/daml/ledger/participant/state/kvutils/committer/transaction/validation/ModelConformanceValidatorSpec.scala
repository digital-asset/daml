// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractState,
  DamlLogEntry,
  DamlStateValue,
  DamlSubmitterInfo,
  DamlTransactionEntry,
  DamlTransactionRejectionEntry,
  InvalidLedgerTime,
}
import com.daml.ledger.participant.state.kvutils.TestHelpers.{createCommitContext, lfTuple}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.ledger.validator.TestHelper.{makeContractIdStateKey, makeContractIdStateValue}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{Engine, Result, ResultError, Error => LfError}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.TransactionOuterClass.ContractInstance
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  ReplayedNodeMissing,
  SubmittedTransaction,
  TransactionVersion,
}
import com.daml.lf.value.Value.{ValueRecord, ValueText}
import com.daml.lf.value.{Value, ValueOuterClass}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ModelConformanceValidatorSpec
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  import ModelConformanceValidatorSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)

  private val mockEngine = mock[Engine]
  private val modelConformanceValidator = new ModelConformanceValidator(mockEngine, metrics)
  private val rejections = new Rejections(metrics)

  private val inputCreate = create(
    inputContractId,
    keyAndMaintainer = Some(inputContractKey -> inputContractKeyMaintainer),
  )
  private val aCreate = create(aContractId)
  private val anotherCreate = create("#anotherContractId")

  private val exercise = txBuilder.exercise(
    contract = inputCreate,
    choice = "DummyChoice",
    consuming = false,
    actingParties = Set(aKeyMaintainer),
    argument = aDummyValue,
    byKey = false,
  )

  private val lookupNodes =
    Seq(aCreate -> true, anotherCreate -> true) map { case (create, found) =>
      txBuilder.lookupByKey(create, found)
    }

  val Seq(aTransaction, anotherTransaction) = lookupNodes map { node =>
    val builder = txBuilder
    val rootId = builder.add(exercise)
    val lookupId = builder.add(node, rootId)
    builder.build() -> lookupId
  }

  private val aTransactionEntry = DamlTransactionEntrySummary(
    DamlTransactionEntry.newBuilder
      .setSubmissionSeed(aSubmissionSeed)
      .setLedgerEffectiveTime(Conversions.buildTimestamp(ledgerEffectiveTime))
      .setTransaction(Conversions.encodeTransaction(aTransaction._1))
      .build
  )

  "createValidationStep" should {
    "create StepContinue in case of correct input" in {
      val mockValidationResult = mock[Result[Unit]]
      when(
        mockValidationResult.consume(
          any[Value.ContractId => Option[
            Value.ContractInst[Value.VersionedValue[Value.ContractId]]
          ]],
          any[Ref.PackageId => Option[Ast.Package]],
          any[GlobalKeyWithMaintainers => Option[Value.ContractId]],
        )
      ).thenReturn(Right(()))
      when(
        mockEngine.validate(
          any[Set[Ref.Party]],
          any[SubmittedTransaction],
          any[Timestamp],
          any[Ref.ParticipantId],
          any[Timestamp],
          any[Hash],
        )
      ).thenReturn(mockValidationResult)

      modelConformanceValidator.createValidationStep(rejections)(
        createCommitContext(
          None,
          Map(
            inputContractIdStateKey -> Some(makeContractIdStateValue()),
            contractIdStateKey1 -> Some(makeContractIdStateValue()),
          ),
        ),
        aTransactionEntry,
      ) shouldBe StepContinue(aTransactionEntry)
    }

    "create StepStop in case of validation error" in {
      when(
        mockEngine.validate(
          any[Set[Ref.Party]],
          any[SubmittedTransaction],
          any[Timestamp],
          any[Ref.ParticipantId],
          any[Timestamp],
          any[Hash],
        )
      ).thenReturn(
        ResultError(
          LfError.Validation.ReplayMismatch(
            ReplayedNodeMissing(aTransaction._1, aTransaction._2, anotherTransaction._1)
          )
        )
      )

      val step = modelConformanceValidator
        .createValidationStep(rejections)(
          createCommitContext(
            None,
            Map(
              inputContractIdStateKey -> Some(makeContractIdStateValue()),
              contractIdStateKey1 -> Some(makeContractIdStateValue()),
            ),
          ),
          aTransactionEntry,
        )
      step shouldBe a[StepStop]
      step
        .asInstanceOf[StepStop]
        .logEntry
        .getTransactionRejectionEntry
        .hasDisputed shouldBe true
    }
  }

  "lookupContract" should {
    "return Some when a contract is present in the current state" in {
      modelConformanceValidator.lookupContract(
        createCommitContext(
          None,
          Map(
            inputContractIdStateKey -> Some(
              aContractIdStateValue
            )
          ),
        )
      )(Conversions.decodeContractId(inputContractId)) shouldBe Some(aContractInst)
    }

    "throw if a contract does not exist in the current state" in {
      a[Err.MissingInputState] should be thrownBy modelConformanceValidator.lookupContract(
        createCommitContext(
          None,
          Map.empty,
        )
      )(Conversions.decodeContractId(inputContractId))
    }
  }

  "lookupKey" should {
    val contractKeyInputs = aTransactionEntry.transaction.contractKeyInputs match {
      case Left(_) => fail()
      case Right(contractKeyInputs) => contractKeyInputs
    }

    "return Some when mapping exists" in {
      modelConformanceValidator.lookupKey(contractKeyInputs)(
        aGlobalKeyWithMaintainers(inputContractKey, inputContractKeyMaintainer)
      ) shouldBe Some(Conversions.decodeContractId(inputContractId))
    }

    "return None when mapping does not exist" in {
      modelConformanceValidator.lookupKey(contractKeyInputs)(
        aGlobalKeyWithMaintainers("nonexistentKey", "nonexistentMaintainer")
      ) shouldBe None
    }
  }

  "validateCausalMonotonicity" should {
    "create StepContinue when causal monotonicity holds" in {
      modelConformanceValidator
        .validateCausalMonotonicity(
          aTransactionEntry,
          createCommitContext(
            None,
            Map(
              inputContractIdStateKey -> Some(makeContractIdStateValue()),
              contractIdStateKey1 -> Some(aStateValueActiveAt(ledgerEffectiveTime.minusSeconds(1))),
            ),
          ),
          rejections,
        ) shouldBe StepContinue(aTransactionEntry)
    }

    "reject transaction when causal monotonicity does not hold" in {
      val step = modelConformanceValidator
        .validateCausalMonotonicity(
          aTransactionEntry,
          createCommitContext(
            None,
            Map(
              inputContractIdStateKey -> Some(makeContractIdStateValue()),
              contractIdStateKey1 -> Some(aStateValueActiveAt(ledgerEffectiveTime.plusSeconds(1))),
            ),
          ),
          rejections,
        )

      val expectedEntry = DamlLogEntry.newBuilder
        .setTransactionRejectionEntry(
          DamlTransactionRejectionEntry.newBuilder
            .setSubmitterInfo(DamlSubmitterInfo.getDefaultInstance)
            .setInvalidLedgerTime(
              InvalidLedgerTime.newBuilder.setDetails("Causal monotonicity violated")
            )
        )
        .build()
      step shouldBe StepStop(expectedEntry)
    }
  }

  private def create(
      contractId: String,
      signatories: Seq[String] = Seq(aKeyMaintainer),
      argument: TransactionBuilder.Value = aDummyValue,
      keyAndMaintainer: Option[(String, String)] = Some(aKey -> aKeyMaintainer),
  ): TransactionBuilder.Create = {
    txBuilder.create(
      id = contractId,
      template = aTemplateId,
      argument = argument,
      signatories = signatories,
      observers = Seq.empty,
      key = keyAndMaintainer.map { case (key, maintainer) => lfTuple(maintainer, key) },
    )
  }
}

object ModelConformanceValidatorSpec {
  private val inputContractId = "#inputContractId"
  private val inputContractIdStateKey = makeContractIdStateKey(inputContractId)
  private val aContractId = "#someContractId"
  private val contractIdStateKey1 = makeContractIdStateKey(aContractId)
  private val inputContractKey = "inputContractKey"
  private val inputContractKeyMaintainer = "inputContractKeyMaintainer"
  private val aKey = "key"
  private val aKeyMaintainer = "maintainer"
  private val aDummyValue = TransactionBuilder.record("field" -> "value")
  private val aTemplateId = "dummyPackage:DummyModule:DummyTemplate"

  private val aSubmissionSeed = ByteString.copyFromUtf8("a" * 32)
  private val ledgerEffectiveTime =
    ZonedDateTime.of(2021, 1, 1, 12, 0, 0, 0, ZoneOffset.UTC).toInstant

  private val aContractIdStateValue = {
    makeContractIdStateValue().toBuilder
      .setContractState(
        DamlContractState
          .newBuilder()
          .setContractInstance(
            ContractInstance
              .newBuilder()
              .setTemplateId(
                ValueOuterClass.Identifier
                  .newBuilder()
                  .setPackageId("dummyPackage")
                  .addModuleName("DummyModule")
                  .addName("DummyTemplate")
              )
              .setArgVersioned(
                ValueOuterClass.VersionedValue
                  .newBuilder()
                  .setVersion(TransactionVersion.VDev.protoValue)
                  .setValue(ValueOuterClass.Value.newBuilder().setText("dummyValue"))
              )
          )
          .build()
      )
      .build()
  }

  private val aContractInst = Value.ContractInst(
    Ref.TypeConName.assertFromString(aTemplateId),
    Value.VersionedValue(TransactionVersion.VDev, ValueText("dummyValue")),
    "",
  )

  private def txBuilder = TransactionBuilder(TransactionVersion.VDev)

  private def aGlobalKeyWithMaintainers(key: String, maintainer: String) = GlobalKeyWithMaintainers(
    GlobalKey.assertBuild(
      Ref.TypeConName.assertFromString(aTemplateId),
      ValueRecord(
        None,
        ImmArray(
          (None, ValueText(maintainer)),
          (None, ValueText(key)),
        ),
      ),
    ),
    Set.empty,
  )

  private def aStateValueActiveAt(activeAt: Instant) =
    DamlStateValue.newBuilder
      .setContractState(
        DamlContractState.newBuilder.setActiveAt(Conversions.buildTimestamp(activeAt))
      )
      .build
}
