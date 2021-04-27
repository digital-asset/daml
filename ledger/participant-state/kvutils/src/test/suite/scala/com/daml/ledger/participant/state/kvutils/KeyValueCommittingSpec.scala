// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{
  SubmitterInfo,
  ApplicationId,
  CommandId,
  TransactionMeta,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlCommandDedupKey}
import com.daml.lf.crypto
import com.daml.lf.data.Time
import com.daml.lf.data.Ref.{Party, Identifier}
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import java.time.Instant

class KeyValueCommitingSpec extends AnyWordSpec with Matchers {
  private val metrics: Metrics = new Metrics(new MetricRegistry)
  private val keyValueSubmission = new KeyValueSubmission(metrics)

  private val alice = Party.assertFromString("Alice")
  private val commandId = CommandId.assertFromString("cmdid")

  private def toSubmission(tx: SubmittedTransaction) = {
    val timestamp = Time.Timestamp.assertFromInstant(Instant.EPOCH)
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
      applicationId = ApplicationId.assertFromString("appid"),
      commandId = commandId,
      deduplicateUntil = Instant.EPOCH,
    )
    keyValueSubmission.transactionToSubmission(
      submitterInfo = submitterInfo,
      meta = meta,
      tx = tx,
    )
  }

  def getOutputs(builder: TransactionBuilder) =
    KeyValueCommitting.submissionOutputs(toSubmission(builder.buildSubmitted()))

  val dedupKey = DamlStateKey.newBuilder
    .setCommandDedup(
      DamlCommandDedupKey.newBuilder.addSubmitters(alice).setCommandId(commandId).build
    )
    .build

  private val keyValue = Value.ValueUnit
  private val templateId = Identifier.assertFromString("pkg:M:T")

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
      val c = create(builder, "#1")
      builder.add(c)
      val key = contractIdToStateKey(c.coid)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        key,
      )
    }
    "return two outputs for a create with a key" in {
      val builder = TransactionBuilder()
      val c = create(builder, "#1", true)
      builder.add(c)
      val contractIdKey = contractIdToStateKey(c.coid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }
    "return a single output for a transient contract" in {
      val builder = TransactionBuilder()
      val c = create(builder, "#1", true)
      builder.add(c)
      builder.add(exercise(builder, "#1"))
      val contractIdKey = contractIdToStateKey(c.coid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }
    "return a single output for an exercise without a key" in {
      val builder = TransactionBuilder()
      val e = exercise(builder, "#1")
      builder.add(e)
      val contractIdKey = contractIdToStateKey(e.targetCoid)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdKey,
      )
    }
    "return two outputs for an exercise with a key" in {
      val builder = TransactionBuilder()
      val e = exercise(builder, "#1", hasKey = true)
      builder.add(e)
      val contractIdKey = contractIdToStateKey(e.targetCoid)
      val contractKeyKey = contractKeyToStateKey(templateId, keyValue)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdKey,
        contractKeyKey,
      )
    }
    "return one output per fetch and fetch-by-key" in {
      val builder = TransactionBuilder()
      val f1 = fetch(builder, "#1", byKey = true)
      val f2 = fetch(builder, "#2", byKey = false)
      builder.add(f1)
      builder.add(f2)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdToStateKey(f1.coid),
        contractIdToStateKey(f2.coid),
      )
    }
    "return no output for a failing lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, "#1", found = false))
      getOutputs(builder) shouldBe Set(dedupKey)

    }
    "return no output for a successful lookup-by-key" in {
      val builder = TransactionBuilder()
      builder.add(lookup(builder, "#1", found = true))
      getOutputs(builder) shouldBe Set(dedupKey)
    }
    "return outputs for nodes under a rollback node" in {
      val builder = TransactionBuilder()
      val rollback = builder.add(builder.rollback())
      val c = create(builder, "#1", hasKey = true)
      builder.add(c, rollback)
      val e = exercise(builder, "#2", hasKey = true)
      builder.add(e, rollback)
      val f = fetch(builder, "#3", byKey = true)
      builder.add(f, rollback)
      getOutputs(builder) shouldBe Set(
        dedupKey,
        contractIdToStateKey(c.coid),
        contractKeyToStateKey(templateId, keyValue),
        contractIdToStateKey(e.targetCoid),
        contractIdToStateKey(f.coid),
      )
    }
  }
}
