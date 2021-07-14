// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import java.time.{Instant, ZoneOffset, ZonedDateTime}

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions
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
import com.daml.ledger.participant.state.v1.{RejectionReason, RejectionReasonV0}
import com.daml.ledger.validator.TestHelper.{makeContractIdStateKey, makeContractIdStateValue}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{Engine, Result, ResultError, Error => LfError}
import com.daml.lf.language.Ast
import com.daml.lf.transaction
import com.daml.lf.transaction.TransactionOuterClass.ContractInstance
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  NodeId,
  RecordedNodeMissing,
  ReplayNodeMismatch,
  ReplayedNodeMissing,
  SubmittedTransaction,
  Transaction,
  TransactionVersion,
}
import com.daml.lf.value.Value.{ValueRecord, ValueText}
import com.daml.lf.value.{Value, ValueOuterClass}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inspectors.forEvery
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

  private val createInput = create(
    inputContractId,
    keyAndMaintainer = Some(inputContractKey -> inputContractKeyMaintainer),
  )
  private val create1 = create(contractId1)
  private val create2 = create("#otherContractId")
  private val otherKeyCreate = create(
    contractId = "#contractWithOtherKey",
    signatories = Seq(aKeyMaintainer),
    keyAndMaintainer = Some("otherKey" -> aKeyMaintainer),
  )

  private val exercise = txBuilder.exercise(
    contract = createInput,
    choice = "DummyChoice",
    consuming = false,
    actingParties = Set(aKeyMaintainer),
    argument = aDummyValue,
    byKey = false,
  )

  val lookupNodes @ Seq(lookup1, lookup2, lookupNone, lookupOther @ _) =
    Seq(create1 -> true, create2 -> true, create1 -> false, otherKeyCreate -> true) map {
      case (create, found) => txBuilder.lookupByKey(create, found)
    }

  val Seq(tx1, tx2, txNone, txOther) = lookupNodes map { node =>
    val builder = txBuilder
    val rootId = builder.add(exercise)
    val lookupId = builder.add(node, rootId)
    builder.build() -> lookupId
  }

  private val transactionEntry1 = DamlTransactionEntrySummary(
    DamlTransactionEntry.newBuilder
      .setSubmissionSeed(aSubmissionSeed)
      .setLedgerEffectiveTime(Conversions.buildTimestamp(ledgerEffectiveTime))
      .setTransaction(Conversions.encodeTransaction(tx1._1))
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
        transactionEntry1,
      ) shouldBe StepContinue(transactionEntry1)
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
      ).thenReturn(ResultError(LfError.Validation.ReplayMismatch(mkMismatch(tx1, tx2))))

      val step = modelConformanceValidator
        .createValidationStep(rejections)(
          createCommitContext(
            None,
            Map(
              inputContractIdStateKey -> Some(makeContractIdStateValue()),
              contractIdStateKey1 -> Some(makeContractIdStateValue()),
            ),
          ),
          transactionEntry1,
        )
      step shouldBe a[StepStop]
      step
        .asInstanceOf[StepStop]
        .logEntry
        .getTransactionRejectionEntry
        .hasInconsistent shouldBe true
    }
  }

  "lookupContract" should {
    "lookup contract" in {
      modelConformanceValidator.lookupContract(
        createCommitContext(
          None,
          Map(
            inputContractIdStateKey -> Some(
              aContractIdStateValue
            )
          ),
        )
      )(Conversions.decodeContractId(inputContractId)) shouldBe a[Some[_]]
    }
  }

  "lookupKey" should {
    val contractKeyInputs = transactionEntry1.transaction.contractKeyInputs match {
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
    "create StepContinue when causal monotonicity is held" in {
      modelConformanceValidator
        .validateCausalMonotonicity(
          transactionEntry1,
          createCommitContext(
            None,
            Map(
              inputContractIdStateKey -> Some(makeContractIdStateValue()),
              contractIdStateKey1 -> Some(aStateValueActiveAt(ledgerEffectiveTime.minusSeconds(1))),
            ),
          ),
          rejections,
        ) shouldBe StepContinue(transactionEntry1)
    }

    "reject transaction when causal monotonicity is not held" in {
      val step = modelConformanceValidator
        .validateCausalMonotonicity(
          transactionEntry1,
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

  "rejectionReasonForValidationError" when {
    "there is a mismatch in lookupByKey nodes" should {
      "report an inconsistency if the contracts are not created in the same transaction" in {
        val inconsistentLookups = Seq(
          mkMismatch(tx1, tx2),
          mkMismatch(tx1, txNone),
          mkMismatch(txNone, tx2),
        )
        forEvery(inconsistentLookups)(checkRejectionReason(RejectionReasonV0.Inconsistent))
      }

      "report Disputed if one of contracts is created in the same transaction" in {
        val Seq(txC1, txC2, txCNone) = Seq(lookup1, lookup2, lookupNone) map { node =>
          val builder = txBuilder
          val rootId = builder.add(exercise)
          builder.add(create1, rootId)
          val lookupId = builder.add(node, rootId)
          builder.build() -> lookupId
        }
        val Seq(tx1C, txNoneC) = Seq(lookup1, lookupNone) map { node =>
          val builder = txBuilder
          val rootId = builder.add(exercise)
          val lookupId = builder.add(node, rootId)
          builder.add(create1)
          builder.build() -> lookupId
        }
        val recordedKeyInconsistent = Seq(
          mkMismatch(txC2, txC1),
          mkMismatch(txCNone, txC1),
          mkMismatch(txC1, txCNone),
          mkMismatch(tx1C, txNoneC),
        )
        forEvery(recordedKeyInconsistent)(checkRejectionReason(RejectionReasonV0.Disputed))
      }

      "report Disputed if the keys are different" in {
        checkRejectionReason(RejectionReasonV0.Disputed)(mkMismatch(txOther, tx1))
      }
    }

    "the mismatch is not between two lookup nodes" should {
      "report Disputed" in {
        val txExerciseOnly = {
          val builder = txBuilder
          builder.add(exercise)
          builder.build()
        }
        val txCreate = {
          val builder = txBuilder
          val rootId = builder.add(exercise)
          val createId = builder.add(create1, rootId)
          builder.build() -> createId
        }
        val miscMismatches = Seq(
          mkMismatch(txCreate, tx1),
          mkRecordedMissing(txExerciseOnly, tx2),
          mkReplayedMissing(tx1, txExerciseOnly),
        )
        forEvery(miscMismatches)(checkRejectionReason(RejectionReasonV0.Disputed))
      }
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

  private def checkRejectionReason(
      mkReason: String => RejectionReason
  )(mismatch: transaction.ReplayMismatch[NodeId, Value.ContractId]) = {
    val replayMismatch = LfError.Validation(LfError.Validation.ReplayMismatch(mismatch))
    ModelConformanceValidator.rejectionReasonForValidationError(replayMismatch) shouldBe mkReason(
      replayMismatch.msg
    )
  }
}

object ModelConformanceValidatorSpec {
  private val inputContractId = "#inputContractId"
  private val inputContractIdStateKey = makeContractIdStateKey(inputContractId)
  private val contractId1 = "#someContractId"
  private val contractIdStateKey1 = makeContractIdStateKey(contractId1)
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
                  .addModuleName("dummyModule")
                  .addName("dummyName")
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

  private def mkMismatch(
      recorded: (Transaction.Transaction, NodeId),
      replayed: (Transaction.Transaction, NodeId),
  ): ReplayNodeMismatch[NodeId, Value.ContractId] =
    ReplayNodeMismatch(recorded._1, recorded._2, replayed._1, replayed._2)
  private def mkRecordedMissing(
      recorded: Transaction.Transaction,
      replayed: (Transaction.Transaction, NodeId),
  ): RecordedNodeMissing[NodeId, Value.ContractId] =
    RecordedNodeMissing(recorded, replayed._1, replayed._2)
  private def mkReplayedMissing(
      recorded: (Transaction.Transaction, NodeId),
      replayed: Transaction.Transaction,
  ): ReplayedNodeMissing[NodeId, Value.ContractId] =
    ReplayedNodeMissing(recorded._1, recorded._2, replayed)
}
