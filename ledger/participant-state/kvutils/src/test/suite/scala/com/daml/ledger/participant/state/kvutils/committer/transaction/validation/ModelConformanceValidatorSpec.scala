// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.TestHelpers.lfTuple
import com.daml.ledger.participant.state.v1.{RejectionReason, RejectionReasonV0}
import com.daml.lf.engine.{Error => LfError}
import com.daml.lf.transaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.{
  NodeId,
  RecordedNodeMissing,
  ReplayNodeMismatch,
  ReplayedNodeMissing,
  Transaction,
}
import com.daml.lf.value.Value
import org.mockito.MockitoSugar
import org.scalatest.Inspectors.forEvery
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ModelConformanceValidatorSpec extends AnyWordSpec with Matchers with MockitoSugar {
  import ModelConformanceValidatorSpec._

  private val txBuilder = TransactionBuilder()

  private val createInput = create("#inputContractId")
  private val create1 = create("#someContractId")
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
    val builder = TransactionBuilder()
    val rootId = builder.add(exercise)
    val lookupId = builder.add(node, rootId)
    builder.build() -> lookupId
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
          val builder = TransactionBuilder()
          val rootId = builder.add(exercise)
          builder.add(create1, rootId)
          val lookupId = builder.add(node, rootId)
          builder.build() -> lookupId
        }
        val Seq(tx1C, txNoneC) = Seq(lookup1, lookupNone) map { node =>
          val builder = TransactionBuilder()
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
          val builder = TransactionBuilder()
          builder.add(exercise)
          builder.build()
        }
        val txCreate = {
          val builder = TransactionBuilder()
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
  ): TransactionBuilder.Create =
    txBuilder.create(
      id = contractId,
      template = "dummyPackage:DummyModule:DummyTemplate",
      argument = argument,
      signatories = signatories,
      observers = Seq.empty,
      key = keyAndMaintainer.map { case (key, maintainer) => lfTuple(maintainer, key) },
    )

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
  private val aKeyMaintainer = "maintainer"
  private val aKey = "key"
  private val aDummyValue = TransactionBuilder.record("field" -> "value")

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
