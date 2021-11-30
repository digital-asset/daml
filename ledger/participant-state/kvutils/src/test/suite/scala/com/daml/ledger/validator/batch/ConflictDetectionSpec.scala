// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionRejectionEntry
import com.daml.ledger.participant.state.kvutils.store.events.PackageUpload.DamlPackageUploadEntry
import com.daml.ledger.participant.state.kvutils.store.{
  DamlContractKey,
  DamlLogEntry,
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.lf.value.ValueOuterClass
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.MockitoSugar
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn

@nowarn("msg=deprecated")
class ConflictDetectionSpec extends AsyncWordSpec with Matchers with Inside with MockitoSugar {
  "detectConflictsAndRecover" should {
    "return output keys as invalidated and unchanged input in case of no conflicts" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val logEntry = aPartyLogEntry("Alice")
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val damlMetrics = metrics()
      val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
      val conflictDetection = new ConflictDetection(damlMetrics)

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit loggingContext =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> None),
            logEntry = logEntry,
            outputState = outputState,
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should be(Set(aliceKey))
      conflictDetectionMetrics.accepted.getCount should be(1)
    }

    "return new output key as invalidated in case of no conflicts" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val conflictDetection = new ConflictDetection(metrics())

      val Some((actualInvalidatedKeys, _)) = LoggingContext.newLoggingContext {
        implicit loggingContext =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map.empty,
            logEntry = aPartyLogEntry("Alice"),
            outputState = outputState,
          )
      }

      actualInvalidatedKeys should be(Set(aliceKey))
    }

    "report conflict for new output key also part of input invalidated key set" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val conflictDetection = new ConflictDetection(metrics())

      val result = LoggingContext.newLoggingContext { implicit loggingContext =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = Set(aliceKey),
          inputState = Map.empty,
          logEntry = aPartyLogEntry("Alice"),
          outputState = outputState,
        )
      }

      result should be(None)
    }

    "drop conflicting party allocation" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val logEntry = aPartyLogEntry("Alice")
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val invalidatedKeys = Set(aliceKey)
      val damlMetrics = metrics()
      val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
      val conflictDetection = new ConflictDetection(damlMetrics)

      val result = LoggingContext.newLoggingContext { implicit loggingContext =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = Map(aliceKey -> None),
          logEntry = logEntry,
          outputState = outputState,
        )
      }

      result should be(None)
      conflictDetectionMetrics.conflicted.getCount should be(1)
      conflictDetectionMetrics.dropped.getCount should be(1)
    }

    "drop conflicting package upload" in {
      val logEntry = DamlLogEntry.newBuilder
        .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
        .build
      val packageUploadKey = DamlStateKey.newBuilder
        .setPackageId("aPackageId")
        .build
      val invalidatedKeys = Set(packageUploadKey)
      val damlMetrics = metrics()
      val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
      val conflictDetection = new ConflictDetection(damlMetrics)

      val result = LoggingContext.newLoggingContext { implicit loggingContext =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = invalidatedKeys.map(_ -> None).toMap,
          logEntry = logEntry,
          outputState = Map.empty,
        )
      }

      result should be(None)
      conflictDetectionMetrics.dropped.getCount should be(1)
    }

    "recover transaction conflicting on contract id" in {
      val txRejectionEntry = conflictingTransactionTest(
        DamlStateKey.newBuilder.setContractId("foo").build
      )
      txRejectionEntry.getInconsistent.getDetails should be("Conflict on contract foo")
    }

    "recover transaction conflicting on contract key" in {
      val txRejectionEntry = conflictingTransactionTest(
        DamlStateKey.newBuilder
          .setContractKey(
            DamlContractKey.newBuilder
              .setHash(ByteString.copyFromUtf8("somehash"))
              .setTemplateId(
                ValueOuterClass.Identifier.newBuilder
                  .addName("Foo")
                  .addModuleName("Bar")
                  .setPackageId("Baz")
              )
          )
          .build
      )
      txRejectionEntry.getInconsistent.getDetails should be(
        "Contract key conflicts in contract template Baz:Bar:Foo"
      )
    }

    "recover transaction conflicting on configuration" in {
      val txRejectionEntry = conflictingTransactionTest(
        DamlStateKey.newBuilder.setConfiguration(com.google.protobuf.Empty.getDefaultInstance).build
      )
      txRejectionEntry.getInconsistent.getDetails should be("Ledger configuration has changed")
    }

    "remove unaltered input key from invalidated keys" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val aStateValue = DamlStateValue.getDefaultInstance
      val outputState = Map(aliceKey -> aStateValue)
      val logEntry = aPartyLogEntry("Alice")
      val damlMetrics = metrics()
      val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
      val conflictDetection = new ConflictDetection(damlMetrics)

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit loggingContext =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> Some(aStateValue)),
            logEntry = logEntry,
            outputState = outputState,
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should not contain aliceKey
      conflictDetectionMetrics.removedTransientKey.getCount should be(1)
      conflictDetectionMetrics.conflicted.getCount should be(0)
      conflictDetectionMetrics.dropped.getCount should be(0)
    }

    "return altered input key in invalidated keys" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val logEntry = aPartyLogEntry("Alice")
      val inputStateValue = DamlStateValue.newBuilder
        .setParty(
          DamlPartyAllocation.newBuilder.setDisplayName("not alice")
        )
        .build
      val outputStateValue = DamlStateValue.getDefaultInstance
      val outputState = Map(aliceKey -> outputStateValue)
      val damlMetrics = metrics()
      val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
      val conflictDetection = new ConflictDetection(damlMetrics)

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit loggingContext =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> Some(inputStateValue)),
            logEntry = logEntry,
            outputState = outputState,
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should be(Set(aliceKey))
      conflictDetectionMetrics.accepted.getCount should be(1)
    }
  }

  private def conflictingTransactionTest(key: DamlStateKey): DamlTransactionRejectionEntry = {
    val damlMetrics = metrics()
    val conflictDetectionMetrics = damlMetrics.daml.kvutils.conflictdetection
    val conflictDetection = new ConflictDetection(damlMetrics)
    val outputState = Map(key -> DamlStateValue.getDefaultInstance)
    val invalidatedKeys = Set(key)
    val Some((_, (newLogEntry, newOutputState))) = LoggingContext.newLoggingContext {
      implicit loggingContext =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = Map(key -> None),
          logEntry = aTransactionLogEntry,
          outputState = outputState,
        )
    }

    newOutputState should be(Map.empty)
    newLogEntry.getPayloadCase should be(DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY)
    val txRejectionEntry = newLogEntry.getTransactionRejectionEntry
    txRejectionEntry.getReasonCase should be(DamlTransactionRejectionEntry.ReasonCase.INCONSISTENT)

    conflictDetectionMetrics.recovered.getCount should be(1)

    txRejectionEntry
  }

  private def metrics(): Metrics = new Metrics(new MetricRegistry)

  private def aPartyLogEntry(party: String): DamlLogEntry = {
    val builder = DamlLogEntry.newBuilder
      .setRecordTime(com.google.protobuf.Timestamp.getDefaultInstance)
    builder.getPartyAllocationEntryBuilder
      .setDisplayName(party)
      .setParty(party)
      .setSubmissionId(s"$party-submission")
    builder.build
  }

  private val aTransactionLogEntry = {
    val builder = DamlLogEntry.newBuilder
      .setRecordTime(com.google.protobuf.Timestamp.getDefaultInstance)
    builder.getTransactionEntryBuilder
      .setLedgerEffectiveTime(com.google.protobuf.Timestamp.getDefaultInstance)
      .setRawTransaction(ByteString.copyFromUtf8("invalid transaction"))
    builder.build
  }

}
