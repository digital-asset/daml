// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.Err.MissingInputState
import com.daml.ledger.participant.state.kvutils.TestHelpers.{
  createCommitContext,
  createEmptyTransactionEntry,
  getTransactionRejectionReason,
  lfTuple,
  mkParticipantId,
  theDefaultConfig,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  TransactionCommitter,
}
import com.daml.ledger.participant.state.kvutils.committer.{StepContinue, StepStop}
import com.daml.ledger.participant.state.v1.{RejectionReason, RejectionReasonV0}
import com.daml.lf.engine.{Engine, Error => LfError}
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
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.mockito.MockitoSugar
import org.scalatest.Inspectors.forEvery
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ModelConformanceValidatorSpec extends AnyWordSpec with Matchers with MockitoSugar {
  import ModelConformanceValidatorSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val transactionCommitter =
    createTransactionCommitter() // Stateless, can be shared between tests
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

  "authorizeSubmitters" should {
    "reject a submission when any of the submitters keys is not present in the input state" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(hostedParty(Bob)),
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob, Emma)))

      a[MissingInputState] should be thrownBy transactionCommitter.authorizeSubmitters(
        context,
        tx,
      )
    }

    "reject a submission when any of the submitters is not known" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> None,
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getPartyNotKnownOnLedger.getDetails
      rejectionReason should fullyMatch regex """Submitting party .+ not known"""
    }

    "reject a submission when any of the submitters' participant id is incorrect" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(notHostedParty(Bob)),
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getSubmitterCannotActViaParticipant.getDetails
      rejectionReason should fullyMatch regex s"""Party .+ not hosted by participant ${mkParticipantId(
        ParticipantId
      )}"""
    }

    "allow a submission when all of the submitters are hosted on the participant" in {
      val context = createCommitContext(
        recordTime = None,
        inputs = createInputs(
          Alice -> Some(hostedParty(Alice)),
          Bob -> Some(hostedParty(Bob)),
          Emma -> Some(hostedParty(Emma)),
        ),
        participantId = ParticipantId,
      )
      val tx = DamlTransactionEntrySummary(createEmptyTransactionEntry(List(Alice, Bob, Emma)))

      val result = transactionCommitter.authorizeSubmitters(context, tx)
      result shouldBe a[StepContinue[_]]
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

  private def createTransactionCommitter(): TransactionCommitter =
    new TransactionCommitter(
      theDefaultConfig,
      mock[Engine],
      metrics,
      inStaticTimeMode = false,
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
  private val Alice = "alice"
  private val Bob = "bob"
  private val Emma = "emma"
  private val ParticipantId = 0
  private val OtherParticipantId = 1

  private val aKeyMaintainer = "maintainer"
  private val aKey = "key"
  private val aDummyValue = TransactionBuilder.record("field" -> "value")

  private def createInputs(
      inputs: (String, Option[DamlPartyAllocation])*
  ): Map[DamlStateKey, Option[DamlStateValue]] =
    inputs.map { case (party, partyAllocation) =>
      DamlStateKey.newBuilder().setParty(party).build() -> partyAllocation
        .map(
          DamlStateValue.newBuilder().setParty(_).build()
        )
    }.toMap

  private def hostedParty(party: String): DamlPartyAllocation =
    partyAllocation(party, ParticipantId)
  private def notHostedParty(party: String): DamlPartyAllocation =
    partyAllocation(party, OtherParticipantId)
  private def partyAllocation(party: String, participantId: Int): DamlPartyAllocation =
    DamlPartyAllocation
      .newBuilder()
      .setParticipantId(mkParticipantId(participantId))
      .setDisplayName(party)
      .build()

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
