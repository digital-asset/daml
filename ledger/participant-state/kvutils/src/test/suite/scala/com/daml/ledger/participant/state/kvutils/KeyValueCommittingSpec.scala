// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.{DeduplicationPeriod => ApiDeduplicationPeriod}
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionEntry
import com.daml.ledger.participant.state.kvutils.store.{DamlCommandDedupKey, DamlStateKey}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.lf.crypto
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
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
      readAs = List.empty,
      applicationId = applicationId,
      commandId = commandId,
      deduplicationPeriod = ApiDeduplicationPeriod.DeduplicationDuration(Duration.ZERO),
      submissionId = None,
      ledgerConfiguration =
        Configuration(1, LedgerTimeModel.reasonableDefault, Duration.ofSeconds(1)),
    )
    keyValueSubmission.transactionToSubmission(
      submitterInfo = submitterInfo,
      meta = meta,
      tx = tx,
    )
  }

  private def toSubmission(builder: TransactionBuilder): DamlSubmission =
    this.toSubmission(builder.buildSubmitted())

  private val contractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("#1"))

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

  private def create(builder: TransactionBuilder) =
    builder.create(
      id = contractId,
      templateId = templateId,
      argument = Value.ValueRecord(None, ImmArray.Empty),
      signatories = Set.empty,
      observers = Set.empty,
      key = Some(keyValue),
    )

  import Conversions.{contractIdToStateKey, contractKeyToStateKey}

  "submissionOutputs" should {
    "return outputs for a create with a key" in {
      val builder = TransactionBuilder()
      val createNode = create(builder)
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

    "fail on a broken transaction" in {
      val brokenTransaction = DamlSubmission
        .newBuilder()
        .setTransactionEntry(
          DamlTransactionEntry.newBuilder().setRawTransaction(ByteString.copyFromUtf8("wrong"))
        )
        .build()
      an[Err.DecodeError] should be thrownBy KeyValueCommitting.submissionOutputs(brokenTransaction)
    }
  }
}
