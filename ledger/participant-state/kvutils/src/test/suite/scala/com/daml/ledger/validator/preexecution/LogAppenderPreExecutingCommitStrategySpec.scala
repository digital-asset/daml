// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlPartyAllocationRejectionEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, `Bytes Ordering`}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.TestHelper.{aLogEntry, aLogEntryId, aParticipantId}
import com.daml.ledger.validator.preexecution.LogAppenderPreExecutingCommitStrategySpec._
import org.mockito.ArgumentMatchers.any
import org.mockito.invocation.InvocationOnMock
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class LogAppenderPreExecutingCommitStrategySpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar {
  "generateWriteSets" should {
    "serialize keys according to strategy" in {
      val mockStateKeySerializationStrategy = mock[StateKeySerializationStrategy]
      when(mockStateKeySerializationStrategy.serializeStateKey(any[DamlStateKey]()))
        .thenAnswer((invocation: InvocationOnMock) =>
          invocation.getArgument[DamlStateKey](0).getContractIdBytes)

      val logEntryId = aLogEntryId()
      val expectedLogEntryKey = logEntryId.toByteString
      val preExecutionResult = PreExecutionResult(
        readSet = Set.empty,
        successfulLogEntry = aLogEntry,
        stateUpdates = (1 to 100).map(key => aStateKey(key) -> aStateValue).toMap,
        outOfTimeBoundsLogEntry = aRejectionLogEntry,
        minimumRecordTime = None,
        maximumRecordTime = None
      )
      val instance = new LogAppenderPreExecutingCommitStrategy(mockStateKeySerializationStrategy)

      instance.generateWriteSets(aParticipantId, logEntryId, Map.empty, preExecutionResult).map {
        actual =>
          verify(mockStateKeySerializationStrategy, times(preExecutionResult.stateUpdates.size))
            .serializeStateKey(any[DamlStateKey])
          actual.successWriteSet.state should have size preExecutionResult.stateUpdates.size.toLong
          actual.successWriteSet.state.map(_._1).toSeq should be(sorted)
          actual.successWriteSet.logEntryKey should be(expectedLogEntryKey)

          actual.outOfTimeBoundsWriteSet.state should have size 0
          actual.outOfTimeBoundsWriteSet.logEntryKey should be(expectedLogEntryKey)

          actual.involvedParticipants shouldBe Set.empty
      }
    }
  }
}

object LogAppenderPreExecutingCommitStrategySpec {
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
