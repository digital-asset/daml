// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.validator.ArgumentMatchers.{anyExecutionContext, anyLoggingContext}
import com.daml.ledger.validator.LogAppendingCommitStrategySpec._
import com.daml.ledger.validator.TestHelper._
import com.daml.logging.LoggingContext
import com.google.protobuf.ByteString
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

final class LogAppendingCommitStrategySpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "commit" should {
    "return index from appendToLog" in {
      val mockLedgerStateOperations = mock[LedgerStateOperations[Long]]
      val expectedIndex = 1234L
      when(
        mockLedgerStateOperations.appendToLog(
          any[Raw.LogEntryId],
          any[Raw.Envelope],
        )(anyExecutionContext, anyLoggingContext)
      ).thenReturn(Future.successful(expectedIndex))
      val instance =
        new LogAppendingCommitStrategy[Long](
          mockLedgerStateOperations,
          DefaultStateKeySerializationStrategy,
        )

      instance
        .commit(aParticipantId, "a correlation ID", aLogEntryId(), aLogEntry, Map.empty, Map.empty)
        .map { actualIndex =>
          verify(mockLedgerStateOperations, times(1))
            .appendToLog(any[Raw.LogEntryId], any[Raw.Envelope])(
              anyExecutionContext,
              anyLoggingContext,
            )
          verify(mockLedgerStateOperations, times(0))
            .writeState(any[Iterable[Raw.StateEntry]])(anyExecutionContext, anyLoggingContext)
          actualIndex should be(expectedIndex)
        }
    }

    "write keys serialized according to strategy" in {
      val mockLedgerStateOperations = mock[LedgerStateOperations[Long]]
      when(
        mockLedgerStateOperations
          .writeState(any[Iterable[Raw.StateEntry]])(anyExecutionContext, anyLoggingContext)
      ).thenReturn(Future.unit)
      when(
        mockLedgerStateOperations.appendToLog(
          any[Raw.LogEntryId],
          any[Raw.Envelope],
        )(anyExecutionContext, anyLoggingContext)
      ).thenReturn(Future.successful(0L))
      val mockStateKeySerializationStrategy = mock[StateKeySerializationStrategy]
      val expectedStateKey = Raw.StateKey(ByteString.copyFromUtf8("some key"))
      when(mockStateKeySerializationStrategy.serializeStateKey(aStateKey))
        .thenReturn(expectedStateKey)
      val expectedOutputStateBytes = Map(expectedStateKey -> Envelope.enclose(aStateValue))
      val instance =
        new LogAppendingCommitStrategy[Long](
          mockLedgerStateOperations,
          mockStateKeySerializationStrategy,
        )

      instance
        .commit(
          aParticipantId,
          "a correlation ID",
          aLogEntryId(),
          aLogEntry,
          Map.empty,
          Map(aStateKey -> aStateValue),
        )
        .map { _: Long =>
          verify(mockStateKeySerializationStrategy, times(1)).serializeStateKey(aStateKey)
          verify(mockLedgerStateOperations, times(1))
            .writeState(eqTo(expectedOutputStateBytes))(anyExecutionContext, anyLoggingContext)
          succeed
        }
    }
  }
}

object LogAppendingCommitStrategySpec {
  private val aStateKey: DamlStateKey = DamlStateKey
    .newBuilder()
    .setContractId(1.toString)
    .build

  private val aStateValue: DamlStateValue = DamlStateValue.getDefaultInstance
}
