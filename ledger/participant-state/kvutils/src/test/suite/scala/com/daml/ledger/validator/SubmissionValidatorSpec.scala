// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Clock

import com.codahale.metrics.MetricRegistry
import com.daml.caching.Cache
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting, Raw}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.ArgumentMatchers.anyExecutionContext
import com.daml.ledger.validator.SubmissionValidatorSpec._
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.google.protobuf.{ByteString, Empty}
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class SubmissionValidatorSpec
    extends AsyncWordSpec
    with Matchers
    with Inside
    with MockitoSugar
    with ArgumentMatchersSugar {
  "validate" should {
    "return success in case of no errors during processing of submission" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val instance = SubmissionValidator.create(
        new FakeStateAccess(mockStateOperations),
        metrics = new Metrics(new MetricRegistry),
        engine = Engine.DevEngine(),
      )
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) {
          case Right(_) => succeed
          case Left(error: ValidationError) => fail(s"ValidationError: $error")
        }
      }
    }

    "signal missing input in case state cannot be retrieved" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(None)))
      val instance = SubmissionValidator.create(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        checkForMissingInputs = true,
        metrics = new Metrics(new MetricRegistry),
        engine = Engine.DevEngine(),
      )
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) { case Left(MissingInputState(keys)) =>
          keys should have size 1
        }
      }
    }

    "return invalid submission for invalid envelope" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      val instance = SubmissionValidator.create(
        new FakeStateAccess(mockStateOperations),
        metrics = new Metrics(new MetricRegistry),
        engine = Engine.DevEngine(),
      )
      instance
        .validate(
          Raw.Envelope(ByteString.copyFrom(Array[Byte](1, 2, 3))),
          "aCorrelationId",
          newRecordTime(),
          aParticipantId(),
        )
        .map {
          inside(_) { case Left(ValidationError(reason)) =>
            reason should include("Failed to parse")
          }
        }
    }

    "return invalid submission in case exception is thrown during processing of submission" in {
      val mockStateOperations = mock[BatchingLedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))

      val failingProcessSubmission: SubmissionValidator.ProcessSubmission =
        (_, _, _, _, _) => throw new IllegalArgumentException("Validation failed")

      val instance = new SubmissionValidator(
        new FakeStateAccess(mockStateOperations),
        failingProcessSubmission,
        allocateLogEntryId = () => aLogEntryId(),
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = new Metrics(new MetricRegistry),
      )
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) { case Left(ValidationError(reason)) =>
          reason should include("Validation failed")
        }
      }
    }
  }

  "validateAndCommit" should {
    "write marshalled log entry to ledger" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val expectedLogResult: Int = 3
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val logEntryIdCaptor = ArgCaptor[Raw.LogEntryId]
      val logEntryValueCaptor = ArgCaptor[Raw.Envelope]
      when(
        mockStateOperations.appendToLog(logEntryIdCaptor.capture, logEntryValueCaptor.capture)(
          anyExecutionContext
        )
      ).thenReturn(Future.successful(expectedLogResult))
      val expectedLogEntryId = aLogEntryId()
      val mockLogEntryIdGenerator = mockFunctionReturning(expectedLogEntryId)
      val metrics = new Metrics(new MetricRegistry)
      val instance = new SubmissionValidator(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        processSubmission = SubmissionValidator
          .processSubmission(new KeyValueCommitting(Engine.DevEngine(), metrics)),
        allocateLogEntryId = mockLogEntryIdGenerator,
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = metrics,
      )
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) { case Right(actualLogResult) =>
            actualLogResult should be(expectedLogResult)
            verify(mockLogEntryIdGenerator, times(1)).apply()
            verify(mockStateOperations, times(0))
              .writeState(any[Iterable[Raw.StateEntry]])(anyExecutionContext)
            logEntryValueCaptor.values should have size 1
            logEntryIdCaptor.values should be(List(Raw.LogEntryId(expectedLogEntryId)))
          }
        }
    }

    "write marshalled key-value pairs to ledger" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val expectedLogResult: Int = 7
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val writtenKeyValuesCaptor = ArgCaptor[Seq[Raw.StateEntry]]
      when(mockStateOperations.writeState(writtenKeyValuesCaptor.capture)(anyExecutionContext))
        .thenReturn(Future.unit)
      val logEntryCaptor = ArgCaptor[Raw.Envelope]
      when(
        mockStateOperations.appendToLog(
          any[Raw.LogEntryId],
          logEntryCaptor.capture,
        )(anyExecutionContext)
      ).thenReturn(Future.successful(expectedLogResult))
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates)
      val instance = new SubmissionValidator(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        processSubmission = (_, _, _, _, _) => _ => logEntryAndStateResult,
        allocateLogEntryId = () => aLogEntryId(),
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = new Metrics(new MetricRegistry),
      )
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) { case Right(actualLogResult) =>
            actualLogResult should be(expectedLogResult)
            writtenKeyValuesCaptor.values should have size 1
            val writtenKeyValues = writtenKeyValuesCaptor.value
            writtenKeyValues should have size 1
            Try(
              SubmissionValidator.stateValueFromRaw(writtenKeyValues.head._2)
            ).isSuccess shouldBe true
            logEntryCaptor.values should have size 1
          }
        }
    }

    "support batch with single submission" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val expectedLogResult: Int = 7
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val writtenKeyValuesCaptor = ArgCaptor[Seq[Raw.StateEntry]]
      when(mockStateOperations.writeState(writtenKeyValuesCaptor.capture)(anyExecutionContext))
        .thenReturn(Future.unit)
      val logEntryCaptor = ArgCaptor[Raw.Envelope]
      when(
        mockStateOperations.appendToLog(
          any[Raw.LogEntryId],
          logEntryCaptor.capture,
        )(anyExecutionContext)
      ).thenReturn(Future.successful(expectedLogResult))
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates)
      val instance = new SubmissionValidator(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        processSubmission = (_, _, _, _, _) => _ => logEntryAndStateResult,
        allocateLogEntryId = () => aLogEntryId(),
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = new Metrics(new MetricRegistry),
      )
      val batchEnvelope =
        Envelope.enclose(
          DamlSubmissionBatch.newBuilder
            .addSubmissions(
              DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId("aCorrelationId")
                .setSubmission(anEnvelope().bytes)
            )
            .build
        )
      instance
        .validateAndCommit(batchEnvelope, "aBatchCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) { case Right(actualLogResult) =>
            actualLogResult should be(expectedLogResult)
            writtenKeyValuesCaptor.values should have size 1
            val writtenKeyValues = writtenKeyValuesCaptor.value
            writtenKeyValues should have size 1
            Try(
              SubmissionValidator.stateValueFromRaw(writtenKeyValues.head._2)
            ).isSuccess shouldBe true
            logEntryCaptor.values should have size 1
          }
        }
    }

    "fail when batch contains more than one submission" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates)
      val instance = new SubmissionValidator(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        processSubmission = (_, _, _, _, _) => _ => logEntryAndStateResult,
        allocateLogEntryId = () => aLogEntryId(),
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = new Metrics(new MetricRegistry),
      )
      val batchEnvelope =
        Envelope.enclose(
          DamlSubmissionBatch.newBuilder
            .addSubmissions(
              DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId("aCorrelationId")
                .setSubmission(anEnvelope().bytes)
            )
            .addSubmissions(
              DamlSubmissionBatch.CorrelatedSubmission.newBuilder
                .setCorrelationId("aCorrelationId2")
                .setSubmission(anEnvelope().bytes)
            )
            .build
        )
      instance
        .validateAndCommit(batchEnvelope, "aBatchCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) { case Left(ValidationError(reason)) =>
            reason should include("Unsupported batch size")
          }
        }
    }

    "return invalid submission if state cannot be written" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      when(mockStateOperations.writeState(any[Iterable[Raw.StateEntry]])(anyExecutionContext))
        .thenThrow(new IllegalArgumentException("Write error"))
      when(mockStateOperations.readState(any[Iterable[Raw.StateKey]])(anyExecutionContext))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      when(
        mockStateOperations.appendToLog(
          any[Raw.LogEntryId],
          any[Raw.Envelope],
        )(anyExecutionContext)
      ).thenReturn(Future.successful(99))
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates)
      val instance = new SubmissionValidator(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        processSubmission = (_, _, _, _, _) => _ => logEntryAndStateResult,
        allocateLogEntryId = () => aLogEntryId(),
        checkForMissingInputs = false,
        stateValueCache = Cache.none,
        metrics = new Metrics(new MetricRegistry),
      )
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) { case Left(ValidationError(reason)) =>
            reason should include("Write error")
          }
        }
    }
  }
}

object SubmissionValidatorSpec {
  import MockitoSugar._

  private def aLogEntry(): DamlLogEntry =
    DamlLogEntry
      .newBuilder()
      .setPartyAllocationEntry(
        DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant")
      )
      .build()

  private def aLogEntryId(): DamlLogEntryId = SubmissionValidator.allocateRandomLogEntryId()

  private def someStateUpdates: Map[DamlStateKey, DamlStateValue] = {
    val key = DamlStateKey
      .newBuilder()
      .setContractId(1.toString)
      .build
    val value = DamlStateValue.getDefaultInstance
    Map(key -> value)
  }

  private def aStateValue(): Raw.Envelope =
    SubmissionValidator.rawEnvelope(DamlStateValue.getDefaultInstance)

  private def anEnvelope(): Raw.Envelope = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addInputDamlState(DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance))
      .build
    Envelope.enclose(submission)
  }

  private def aParticipantId(): ParticipantId = ParticipantId.assertFromString("aParticipantId")

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def mockFunctionReturning[A](returnValue: A): () => A = {
    val mockFunction = mock[() => A]
    when(mockFunction.apply()).thenReturn(returnValue)
    mockFunction
  }

  private class FakeStateAccess[LogResult](mockStateOperations: LedgerStateOperations[LogResult])
      extends LedgerStateAccess[LogResult] {
    override def inTransaction[T](
        body: LedgerStateOperations[LogResult] => Future[T]
    )(implicit executionContext: ExecutionContext): Future[T] =
      body(mockStateOperations)
  }
}
