// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.lf.value.{ValueOuterClass}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.{AsyncWordSpec, Inside, Matchers}
import org.scalatest.mockito.MockitoSugar

class ConflictDetectionSpec extends AsyncWordSpec with Matchers with Inside with MockitoSugar {
  val metrics: () => Metrics = () => new Metrics(new MetricRegistry)

  "conflictDetectAndRecover" should {
    "return output keys as invalidated and unchanged input in case of no conflicts" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val logEntry = aPartyLogEntry("Alice")
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val conflictDetection = new ConflictDetection(metrics())
      val expectedAcceptedCount = conflictDetection.Metrics.accepted.getCount + 1

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit logCtx =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> None),
            logEntry = logEntry,
            outputState = outputState
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should contain(aliceKey)
      conflictDetection.Metrics.accepted.getCount should be(expectedAcceptedCount)
    }

    "return new output key as invalidated in case of no conflicts" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val conflictDetection = new ConflictDetection(metrics())

      val Some((actualInvalidatedKeys, _)) = LoggingContext.newLoggingContext { implicit logCtx =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = Set.empty,
          inputState = Map.empty,
          logEntry = aPartyLogEntry("Alice"),
          outputState = outputState
        )
      }

      actualInvalidatedKeys should contain(aliceKey)
    }

    "report conflict for new output key also part of input invalidated key set" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val conflictDetection = new ConflictDetection(metrics())

      val result = LoggingContext.newLoggingContext { implicit logCtx =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = Set(aliceKey),
          inputState = Map.empty,
          logEntry = aPartyLogEntry("Alice"),
          outputState = outputState
        )
      }

      result should be(None)
    }

    "drop conflicting party allocation" in {
      val aliceKey = DamlStateKey.newBuilder.setParty("Alice").build
      val logEntry = aPartyLogEntry("Alice")
      val outputState = Map(aliceKey -> DamlStateValue.getDefaultInstance)
      val invalidatedKeys = Set(aliceKey)
      val conflictDetection = new ConflictDetection(metrics())
      val expectedConflictedCount = conflictDetection.Metrics.conflicted.getCount + 1
      val expectedDroppedCount = conflictDetection.Metrics.dropped.getCount + 1

      val result = LoggingContext.newLoggingContext { implicit logCtx =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = Map(aliceKey -> None),
          logEntry = logEntry,
          outputState = outputState
        )
      }

      result should be(None)
      conflictDetection.Metrics.conflicted.getCount should be(expectedConflictedCount)
      conflictDetection.Metrics.dropped.getCount should be(expectedDroppedCount)
    }

    "drop conflicting package upload" in {
      val logEntry = DamlLogEntry.newBuilder
        .setPackageUploadEntry(DamlPackageUploadEntry.getDefaultInstance)
        .build
      val packageUploadKey = DamlStateKey.newBuilder
        .setPackageId("aPackageId")
        .build
      val invalidatedKeys = Set(packageUploadKey)
      val conflictDetection = new ConflictDetection(metrics())
      val expectedDroppedCount = conflictDetection.Metrics.dropped.getCount + 1

      val result = LoggingContext.newLoggingContext { implicit logCtx =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = invalidatedKeys.map(_ -> None).toMap,
          logEntry = logEntry,
          outputState = Map.empty
        )
      }

      result should be(None)
      conflictDetection.Metrics.dropped.getCount should be(expectedDroppedCount)
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
                  .setPackageId("Baz"))
          )
          .build
      )
      txRejectionEntry.getInconsistent.getDetails should be(
        "Contract key conflicts in contract template Baz:Bar:Foo")
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
      val conflictDetection = new ConflictDetection(metrics())
      val expectedConflictedTransientCount = conflictDetection.Metrics.removedTransientKey.getCount + 1
      val expectedConflictedCount = conflictDetection.Metrics.conflicted.getCount
      val expectedDroppedCount = conflictDetection.Metrics.dropped.getCount

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit logCtx =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> Some(aStateValue)),
            logEntry = logEntry,
            outputState = outputState
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should not contain aliceKey
      conflictDetection.Metrics.removedTransientKey.getCount should be(
        expectedConflictedTransientCount)
      conflictDetection.Metrics.conflicted.getCount should be(expectedConflictedCount)
      conflictDetection.Metrics.dropped.getCount should be(expectedDroppedCount)
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
      val conflictDetection = new ConflictDetection(metrics())
      val expectedAcceptedCount = conflictDetection.Metrics.accepted.getCount + 1

      val Some((actualInvalidatedKeys, result)) = LoggingContext.newLoggingContext {
        implicit logCtx =>
          conflictDetection.detectConflictsAndRecover(
            invalidatedKeys = Set.empty,
            inputState = Map(aliceKey -> Some(inputStateValue)),
            logEntry = logEntry,
            outputState = outputState
          )
      }

      result should be(logEntry -> outputState)
      actualInvalidatedKeys should contain(aliceKey)
      conflictDetection.Metrics.accepted.getCount should be(expectedAcceptedCount)
    }
  }

  private def conflictingTransactionTest(key: DamlStateKey): DamlTransactionRejectionEntry = {
    val conflictDetection = new ConflictDetection(metrics())
    val outputState = Map(key -> DamlStateValue.getDefaultInstance)
    val invalidatedKeys = Set(key)
    val expectedRecoveredCount = conflictDetection.Metrics.recovered.getCount + 1
    val Some((_, (newLogEntry, newOutputState))) = LoggingContext.newLoggingContext {
      implicit logCtx =>
        conflictDetection.detectConflictsAndRecover(
          invalidatedKeys = invalidatedKeys,
          inputState = Map(key -> None),
          logEntry = aTransactionLogEntry,
          outputState = outputState
        )
    }

    newOutputState should be(Map.empty)
    newLogEntry.getPayloadCase should be(DamlLogEntry.PayloadCase.TRANSACTION_REJECTION_ENTRY)
    val txRejectionEntry = newLogEntry.getTransactionRejectionEntry
    txRejectionEntry.getReasonCase should be(DamlTransactionRejectionEntry.ReasonCase.INCONSISTENT)

    conflictDetection.Metrics.recovered.getCount should be(expectedRecoveredCount)

    txRejectionEntry
  }

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
      .getTransactionBuilder
      .addRoots("foo")
    builder.build
  }

}
