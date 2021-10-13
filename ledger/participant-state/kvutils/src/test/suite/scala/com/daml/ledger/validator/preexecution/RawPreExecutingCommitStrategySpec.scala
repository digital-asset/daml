// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.preexecution

import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlPartyAllocationRejectionEntry,
  Duplicate,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlContractState,
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.daml.ledger.validator.TestHelper.{
  aLogEntry,
  aLogEntryId,
  aParticipantId,
  allDamlStateKeyTypes,
}
import com.daml.ledger.validator.preexecution.RawPreExecutingCommitStrategySpec._
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class RawPreExecutingCommitStrategySpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar {

  "generateReadSet" should {
    "generate a read set" in {
      val contractIdStateKey = DamlStateKey.newBuilder.setContractId("a contract ID").build
      val contractIdStateValue =
        DamlStateValue.newBuilder.setContractState(DamlContractState.newBuilder).build

      val mockStateKeySerializationStrategy = newMockStateKeySerializationStrategy
      val instance = new RawPreExecutingCommitStrategy(mockStateKeySerializationStrategy)

      instance.generateReadSet(
        fetchedInputs = Map(contractIdStateKey -> Some(contractIdStateValue)),
        accessedKeys = Set(contractIdStateKey),
      ) should be(Map(contractIdStateKey -> Some(contractIdStateValue)))
    }

    "throw in case an input key is declared in the read set but not fetched as input" in {
      val mockStateKeySerializationStrategy = newMockStateKeySerializationStrategy
      val instance = new RawPreExecutingCommitStrategy(mockStateKeySerializationStrategy)

      assertThrows[IllegalStateException](
        instance
          .generateReadSet(
            fetchedInputs = Map.empty,
            accessedKeys = allDamlStateKeyTypes.toSet,
          )
      )
    }
  }

  "generateWriteSets" should {
    "serialize keys according to strategy" in {
      val logEntryId = aLogEntryId()
      val expectedLogEntryKey = Raw.LogEntryId(logEntryId)
      val preExecutionResult = PreExecutionResult(
        readSet = Set.empty,
        successfulLogEntry = aLogEntry,
        stateUpdates = (1 to 100).map(key => aStateKey(key) -> aStateValue).toMap,
        outOfTimeBoundsLogEntry = aRejectionLogEntry,
        minimumRecordTime = None,
        maximumRecordTime = None,
      )

      val mockStateKeySerializationStrategy = newMockStateKeySerializationStrategy
      val instance = new RawPreExecutingCommitStrategy(mockStateKeySerializationStrategy)

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

  private def newMockStateKeySerializationStrategy = {
    val mockStateKeySerializationStrategy = mock[StateKeySerializationStrategy]
    when(mockStateKeySerializationStrategy.serializeStateKey(any[DamlStateKey]()))
      .thenAnswer((invocation: InvocationOnMock) =>
        Raw.StateKey(invocation.getArgument[DamlStateKey](0).getContractIdBytes)
      )
    mockStateKeySerializationStrategy
  }
}

object RawPreExecutingCommitStrategySpec {
  private def aStateKey(id: Int) =
    DamlStateKey
      .newBuilder()
      .setContractId(id.toString)
      .build

  private val aStateValue = DamlStateValue.getDefaultInstance

  private val aRejectionLogEntry = DamlLogEntry.newBuilder
    .setPartyAllocationRejectionEntry(
      DamlPartyAllocationRejectionEntry.newBuilder
        .setDuplicateSubmission(Duplicate.getDefaultInstance)
        .setParticipantId("a participant ID")
        .setSubmissionId("a submission")
    )
    .build
}
