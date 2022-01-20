// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.TestHelpers.{
  createCommitContext,
  createTransactionEntry,
  getTransactionRejectionReason,
  lfTuple,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
}
import com.daml.ledger.participant.state.kvutils.committer.{
  CommitContext,
  StepContinue,
  StepResult,
  StepStop,
}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionRejectionEntry
import com.daml.ledger.participant.state.kvutils.store.{
  DamlContractKey,
  DamlContractKeyState,
  DamlContractState,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.validator.TestHelper.{makeContractIdStateKey, makeContractIdStateValue}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.Timestamp
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.wordspec.AnyWordSpec

class TransactionConsistencyValidatorSpec extends AnyWordSpec with Matchers {
  import TransactionBuilder.Implicits._
  import TransactionConsistencyValidatorSpec._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private val metrics = new Metrics(new MetricRegistry)
  private val rejections = new Rejections(metrics)
  private val txBuilder = TransactionBuilder()

  private val conflictingKey = {
    val aCreateNode = newCreateNodeWithFixedKey(Value.ContractId.V1(Hash.hashPrivateKey("#dummy")))
    Conversions.encodeContractKey(aCreateNode.templateId, aCreateNode.key.get.key)
  }

  "TransactionConsistencyValidator" should {
    "return Inconsistent when a contract key resolves to a different contract ID than submitted by a participant" in {
      val cases =
        Seq(
          ("existing global key was not found", false, Some(s"#$freshContractId")),
          (
            "existing global key was mapped to the wrong contract id",
            true,
            Some(s"#$freshContractId"),
          ),
          ("no global key exists but lookup succeeded", true, None),
        )
          .flatMap { case (name, found, contractIdAtCommitter) =>
            Seq(false, true).map(inRollback =>
              (name, newLookupByKeySubmittedTransaction(found, inRollback), contractIdAtCommitter)
            )
          }

      val casesTable = Table(
        ("name", "transaction", "contractIdAtCommitter"),
        cases: _*
      )

      forAll(casesTable) {
        (_, transaction: SubmittedTransaction, contractIdAtCommitter: Option[String]) =>
          val context = commitContextWithContractStateKeys(
            conflictingKey -> contractIdAtCommitter
          )
          val result = validate(context, transaction)
          result shouldBe a[StepStop]

          val rejectionReason =
            getTransactionRejectionReason(result).getReasonCase
          rejectionReason should be(
            DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
          )
      }
    }

    "return DuplicateKeys when two local contracts conflict" in {
      val builder = TransactionBuilder()
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS)
    }

    "return DuplicateKeys when a local contract conflicts with a global contract" in {
      val builder = TransactionBuilder()
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> Some(freshContractId.coid))
      val result = validate(context, transaction)
      result shouldBe a[StepStop]
      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS)
    }

    "succeeds when a global contract gets archived before a local contract gets created" in {
      val globalCid = freshContractId
      val globalCreate = newCreateNodeWithFixedKey(globalCid)
      val context = createCommitContext(
        recordTime = None,
        inputs = Map(
          makeContractIdStateKey(globalCid.coid) -> Some(makeContractIdStateValue()),
          contractStateKey(conflictingKey) -> Some(contractKeyStateValue(globalCid.coid)),
        ),
      )
      val builder = TransactionBuilder()
      builder.add(archive(globalCreate, Set("Alice")))
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val result = validate(context, transaction)
      result shouldBe a[StepContinue[_]]
    }

    "succeeds when a local contract gets archived before another local contract gets created" in {
      val localCid = freshContractId
      val context = commitContextWithContractStateKeys(conflictingKey -> None)
      val builder = TransactionBuilder()
      val localCreate = newCreateNodeWithFixedKey(localCid)
      builder.add(localCreate)
      builder.add(archive(localCreate, Set("Alice")))
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val result = validate(context, transaction)
      result shouldBe a[StepContinue[_]]
    }

    "return DuplicateKeys when a create in a rollback conflicts with a global key" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(freshContractId), rollback)
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> Some(freshContractId.coid))

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS)
    }

    "not return DuplicateKeys between local contracts if first create is rolled back" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(freshContractId), rollback)
      builder.add(newCreateNodeWithFixedKey(freshContractId))

      val transaction = builder.buildSubmitted()

      val context = commitContextWithContractStateKeys(conflictingKey -> None)
      val result = validate(context, transaction)
      result shouldBe a[StepContinue[_]]
    }

    "return DuplicateKeys between local contracts even if second create is rolled back" in {
      val builder = TransactionBuilder()
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val rollback = builder.add(builder.rollback())
      builder.add(newCreateNodeWithFixedKey(freshContractId), rollback)
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS)
    }

    "return DuplicateKeys between local contracts even if the first one was archived in a rollback" in {
      val builder = TransactionBuilder()
      val create = newCreateNodeWithFixedKey(freshContractId)
      builder.add(create)
      val rollback = builder.add(builder.rollback())
      builder.add(archive(create, Set("Alice")), rollback)
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> None)

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS)
    }

    "return InconsistentKeys on conflict local and global contracts even if global was archived in a rollback" in {
      val builder = TransactionBuilder()
      val globalCid = freshContractId
      val rollback = builder.add(builder.rollback())
      builder.add(archive(globalCid, Set("Alice")), rollback)
      builder.add(newCreateNodeWithFixedKey(freshContractId))
      val transaction = builder.buildSubmitted()
      val context = commitContextWithContractStateKeys(conflictingKey -> Some(globalCid.coid))

      val result = validate(context, transaction)
      result shouldBe a[StepStop]

      val rejectionReason =
        getTransactionRejectionReason(result).getReasonCase
      rejectionReason should be(
        DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
      )
    }

    "fail if a contract is not active anymore" in {
      val globalCid = freshContractId
      val globalCreate = newCreateNodeWithFixedKey(globalCid)
      val context = createCommitContext(
        recordTime = None,
        inputs = Map(
          makeContractIdStateKey(globalCid.coid) -> Some(
            makeContractIdStateValue().toBuilder
              .setContractState(
                DamlContractState.newBuilder().setArchivedAt(Timestamp.getDefaultInstance)
              )
              .build()
          )
        ),
      )
      val builder = TransactionBuilder()
      builder.add(archive(globalCreate, Set("Alice")))
      val transaction = builder.buildSubmitted()
      val result = validate(context, transaction)
      inside(result) { case StepStop(logEntry) =>
        logEntry.getTransactionRejectionEntry.getReasonCase shouldBe DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS
      }
    }
  }

  private def newLookupByKeySubmittedTransaction(
      found: Boolean,
      inRollback: Boolean,
  ): SubmittedTransaction = {
    val lookup =
      txBuilder.lookupByKey(newCreateNodeWithFixedKey(contractId = freshContractId), found)
    val builder = TransactionBuilder()
    if (inRollback) {
      val rollback = builder.add(txBuilder.rollback())
      builder.add(lookup, rollback)
    } else {
      builder.add(lookup)
    }
    builder.buildSubmitted()
  }

  private def newCreateNodeWithFixedKey(contractId: Value.ContractId): Node.Create =
    create(contractId, signatories = Set("Alice"), keyAndMaintainer = Some(aKey -> "Alice"))

  private def create(
      contractId: Value.ContractId,
      signatories: Set[Ref.Party] = Set(aKeyMaintainer),
      argument: Value = aDummyValue,
      keyAndMaintainer: Option[(String, String)] = Some(aKey -> aKeyMaintainer),
  ): Node.Create =
    txBuilder.create(
      id = contractId,
      templateId = "DummyModule:DummyTemplate",
      argument = argument,
      signatories = signatories,
      observers = Set.empty,
      key = keyAndMaintainer.map { case (key, maintainer) => lfTuple(maintainer, key) },
    )

  private def archive(create: Node.Create, actingParties: Set[Ref.Party]): Node.Exercise =
    txBuilder.exercise(
      create,
      choice = "Archive",
      consuming = true,
      actingParties = actingParties,
      argument = Value.ValueRecord(None, ImmArray.Empty),
      result = Some(Value.ValueUnit),
    )

  private def archive(contractId: Value.ContractId, actingParties: Set[Ref.Party]): Node.Exercise =
    archive(create(contractId), actingParties)

  private def validate(
      ctx: CommitContext,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    TransactionConsistencyValidator.createValidationStep(rejections)(
      ctx,
      DamlTransactionEntrySummary(createTransactionEntry(List("Alice"), transaction)),
    )
  }
}

object TransactionConsistencyValidatorSpec {
  private val aKeyMaintainer = "maintainer"
  private val aKey = "key"
  private val aDummyValue = TransactionBuilder.record("field" -> "value")

  private def freshContractId: Value.ContractId =
    Value.ContractId.V1(Hash.hashPrivateKey(UUID.randomUUID.toString))

  private def commitContextWithContractStateKeys(
      contractKeyIdPairs: (DamlContractKey, Option[String])*
  ): CommitContext =
    createCommitContext(
      recordTime = None,
      inputs = contractKeyIdPairs.map { case (key, id) =>
        contractStateKey(key) -> id.map(contractKeyStateValue)
      }.toMap,
    )

  private def contractStateKey(contractKey: DamlContractKey): DamlStateKey =
    DamlStateKey
      .newBuilder()
      .setContractKey(contractKey)
      .build()

  private def contractKeyStateValue(contractId: String): DamlStateValue =
    DamlStateValue
      .newBuilder()
      .setContractKeyState(contractKeyState(contractId))
      .build()

  private def contractKeyState(contractId: String): DamlContractKeyState =
    DamlContractKeyState
      .newBuilder()
      .setContractId(contractId)
      .build()
}
