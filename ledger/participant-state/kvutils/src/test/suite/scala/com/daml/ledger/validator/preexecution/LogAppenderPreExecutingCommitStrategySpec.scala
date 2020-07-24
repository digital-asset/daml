// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlPartyAllocationRejectionEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.TestHelper.{aLogEntry, aLogEntryId, aParticipantId}
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

class LogAppenderPreExecutingCommitStrategySpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar {
  "generateWriteSets" should {
    "serialize keys according to strategy" in {
      val mockStateKeySerializationStrategy = mock[StateKeySerializationStrategy]
      val expectedStateKey = ByteString.copyFromUtf8("some key")
      when(mockStateKeySerializationStrategy.serializeStateKey(any[DamlStateKey]()))
        .thenReturn(expectedStateKey)
      val logEntryId = aLogEntryId()
      val expectedLogEntryKey = ByteString.copyFromUtf8("L").concat(logEntryId.toByteString)
      val preExecutionResult = PreExecutionResult(
        readSet = Set.empty,
        successfulLogEntry = aLogEntry,
        stateUpdates = Map(aStateKey(0) -> aStateValue, aStateKey(1) -> aStateValue),
        outOfTimeBoundsLogEntry = aRejectionLogEntry,
        minimumRecordTime = None,
        maximumRecordTime = None
      )
      val instance = new LogAppenderPreExecutingCommitStrategy(mockStateKeySerializationStrategy)

      instance.generateWriteSets(aParticipantId, logEntryId, Map.empty, preExecutionResult).map {
        actual =>
          verify(mockStateKeySerializationStrategy, times(preExecutionResult.stateUpdates.size))
            .serializeStateKey(any[DamlStateKey])
          actual.successWriteSet should have size (preExecutionResult.stateUpdates.size + 1L)
          actual.successWriteSet.toMap.keys should contain(expectedLogEntryKey)

          actual.outOfTimeBoundsWriteSet should have size 1
          actual.outOfTimeBoundsWriteSet.toMap.keys should contain(expectedLogEntryKey)

          actual.involvedParticipants shouldBe Set.empty
      }
    }
  }

  private def aStateKey(id: Int) =
    DamlStateKey
      .newBuilder()
      .setContractId(id.toString)
      .build

  private val aStateValue = DamlStateValue.getDefaultInstance

  private val aRejectionLogEntry = DamlLogEntry.newBuilder
    .setPartyAllocationRejectionEntry(
      DamlPartyAllocationRejectionEntry.newBuilder
        .setDuplicateSubmission(DamlKvutils.Duplicate.getDefaultInstance)
        .setParticipantId("a participant ID")
        .setSubmissionId("a submission"))
    .build
}
