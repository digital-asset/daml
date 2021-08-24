// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlCommandDedupKey, DamlStateKey}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KeyValueCommittingSpec extends AnyWordSpec with Matchers {
  private val metrics: Metrics = new Metrics(new MetricRegistry)
  private val keyValueSubmission = new KeyValueSubmission(metrics)

  private val alice = Ref.Party.assertFromString("Alice")
  private val commandId = Ref.CommandId.assertFromString("cmdid")
  private val applicationId = Ref.ApplicationId.assertFromString("appid")

  private def toSubmission(tx: SubmittedTransaction): DamlSubmission = {
    val timestamp = Time.Timestamp.Epoch
    val meta = TransactionMeta(
      ledgerEffectiveTime = timestamp,
      workflowId = None,
      submissionTime = timestamp,
      submissionSeed = crypto.Hash.hashPrivateKey(this.getClass.getName),
      optUsedPackages = Some(Set.empty),
      optNodeSeeds = None,
      optByKeyNodes = None,
    )
    val submitterInfo = SubmitterInfo(
      actAs = List(alice),
      applicationId = applicationId,
      commandId = commandId,
      deduplicationPeriod = api.DeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      submissionId = Ref.LedgerString.assertFromString("submission"),
      ledgerConfiguration = null,
    )
    keyValueSubmission.transactionToSubmission(
      submitterInfo = submitterInfo,
      meta = meta,
      tx = tx,
    )
  }

  private def toSubmission(builder: TransactionBuilder): DamlSubmission =
    this.toSubmission(builder.buildSubmitted())

  private val dedupKey = DamlStateKey.newBuilder
    .setCommandDedup(
      DamlCommandDedupKey.newBuilder
        .addSubmitters(alice)
        .setApplicationId(applicationId)
        .setCommandId(commandId)
        .build
    )
    .build

  private val keyValue = Value.ValueUnit
  private val templateId = Ref.Identifier.assertFromString("pkg:M:T")

  private def create(builder: TransactionBuilder, id: String, hasKey: Boolean = false) =
    builder.create(
      id = id,
      template = templateId.toString,
      argument = Value.ValueRecord(None, ImmArray.empty),
      signatories = Seq(),
      observers = Seq(),
      key = if (hasKey) Some(keyValue) else None,
    )

  private def exercise(
      builder: TransactionBuilder,
      id: String,
      hasKey: Boolean = false,
      consuming: Boolean = true,
  ) =
    builder.exercise(
      contract = create(builder, id, hasKey),
      choice = "Choice",
      argument = Value.ValueRecord(None, ImmArray.empty),
      actingParties = Set(),
      consuming = consuming,
      result = Some(Value.ValueUnit),
    )

  private def fetch(builder: TransactionBuilder, id: String, byKey: Boolean) =
    builder.fetch(contract = create(builder, id, hasKey = true), byKey = byKey)

  private def lookup(builder: TransactionBuilder, id: String, found: Boolean) =
    builder.lookupByKey(contract = create(builder, id, hasKey = true), found = found)

  import Conversions.{contractIdToStateKey, contractKeyToStateKey}

  "submissionOutputs" should {

    "return a single output for a create without a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, "#1")
      builder.add(createNode)
      val key = contractIdToStateKey(createNode.coid)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        key,
      )
    }

    "return two outputs for a create with a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, "#1", true)
      builder.add(createNode)
      val contractIdKey = contractIdToStateKey(createNode.coid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }

    "return a single output for a transient contract" in {
      val builder = TransactionBuilder()
      val createNode = create(builder, "#1", true)
      builder.add(createNode)
      builder.add(exercise(builder, "#1"))
      val contractIdKey = contractIdToStateKey(createNode.coid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }

    "return a single output for an exercise without a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, "#1")
      builder.add(exerciseNode)
      val contractIdKey = contractIdToStateKey(exerciseNode.targetCoid)
      KeyValueCommitting.submissionOutputs(toSubmission(builder)) shouldBe Set(
        dedupKey,
        contractIdKey,
      )
    }

    "return two outputs for an exercise with a key" in {
      val builder = TransactionBuilder()
      val exerciseNode = exercise(builder, "#1", hasKey = true)
      builder.add(exerciseNode)
      val contractIdKey = contractIdToStateKey(exerciseNode.targetCoid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }

    "return one output per fetch and fetch-by-key" in {
      val builder = TransactionBuilder()
      val fetchNode1 = fetch(builder, "#1", byKey = true)
      val fetchNode2 = fetch(builder, "#2", byKey = false)
      builder.add(fetchNode1)
      builder.add(fetchNode2)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        contractIdToStateKey(fetchNode1.coid),
        contractIdToStateKey(fetchNode2.coid),
      )
    }

    "return no output for a failing lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, "#1", found = false))
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(dedupKey)
    }

    "return no output for a successful lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, "#1", found = true))
      KeyValueCommitting.submissionOutputs(toSubmission(builder)) shouldBe Set(dedupKey)
    }

    "return outputs for nodes under a rollback node" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      val createNode = create(builder, "#1", hasKey = true)
      builder.add(createNode, rollback)
      val exerciseNode = exercise(builder, "#2", hasKey = true)
      builder.add(exerciseNode, rollback)
      val fetchNode = fetch(builder, "#3", byKey = true)
      builder.add(fetchNode, rollback)
      val result = KeyValueCommitting.submissionOutputs(toSubmission(builder))
      result shouldBe Set(
        dedupKey,
        contractIdToStateKey(createNode.coid),
        contractKeyToStateKey(templateId, keyValue),
        contractIdToStateKey(exerciseNode.targetCoid),
        contractIdToStateKey(fetchNode.coid),
      )
    }
  }
}
