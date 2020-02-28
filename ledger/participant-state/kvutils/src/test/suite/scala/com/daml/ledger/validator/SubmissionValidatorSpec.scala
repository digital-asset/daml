// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.time.Clock

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.validator.SubmissionValidator.{LogEntryAndState, RawBytes, RawKeyValuePairs}
import com.daml.ledger.validator.ValidationFailed.{MissingInputState, ValidationError}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.{ByteString, Empty}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Inside, Matchers}

import scala.concurrent.Future
import scala.util.Try

class SubmissionValidatorSpec extends AsyncWordSpec with Matchers with MockitoSugar with Inside {
  "validate" should {
    "return success in case of no errors during processing of submission" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val instance = SubmissionValidator.create(new FakeStateAccess(mockStateOperations))
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) {
          case Right(_) => succeed
        }
      }
    }

    "signal missing input in case state cannot be retrieved" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(None)))
      val instance = SubmissionValidator.create(
        ledgerStateAccess = new FakeStateAccess(mockStateOperations),
        checkForMissingInputs = true)
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) {
          case Left(MissingInputState(keys)) => keys should have size 1
        }
      }
    }

    "return invalid submission for invalid envelope" in {
      val mockStateOperations = mock[LedgerStateOperations[Unit]]
      val instance = SubmissionValidator.create(new FakeStateAccess(mockStateOperations))
      instance
        .validate(Array[Byte](1, 2, 3), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) {
            case Left(ValidationError(reason)) => reason should include("Failed to parse")
          }
        }
    }

    "return invalid submission in case exception is thrown during processing of submission" in {
      val mockStateOperations = mock[BatchingLedgerStateOperations[Unit]]
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))

      def failingProcessSubmission(
          damlLogEntryId: DamlLogEntryId,
          recordTime: Timestamp,
          damlSubmission: DamlSubmission,
          participantId: ParticipantId,
          inputState: Map[DamlStateKey, Option[DamlStateValue]]
      ): LogEntryAndState =
        throw new IllegalArgumentException("Validation failed")

      val instance =
        new SubmissionValidator(
          new FakeStateAccess(mockStateOperations),
          failingProcessSubmission,
          () => aLogEntryId())
      instance.validate(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId()).map {
        inside(_) {
          case Left(ValidationError(reason)) => reason should include("Validation failed")
        }
      }
    }
  }

  "validateAndCommit" should {
    "write marshalled log entry to ledger" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val expectedLogResult: Int = 3
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val logEntryValueCaptor = ArgumentCaptor.forClass(classOf[RawBytes])
      val logEntryIdCaptor = ArgumentCaptor.forClass(classOf[RawBytes])
      when(
        mockStateOperations.appendToLog(logEntryIdCaptor.capture(), logEntryValueCaptor.capture()))
        .thenReturn(Future.successful(expectedLogResult))
      val expectedLogEntryId = aLogEntryId()
      val mockLogEntryIdGenerator = mockFunctionReturning(expectedLogEntryId)
      val instance = new SubmissionValidator(
        new FakeStateAccess(mockStateOperations),
        SubmissionValidator.processSubmission,
        mockLogEntryIdGenerator)
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) {
            case Right(actualLogResult) =>
              actualLogResult should be(expectedLogResult)
              verify(mockLogEntryIdGenerator, times(1)).apply()
              verify(mockStateOperations, times(0)).writeState(any[RawKeyValuePairs]())
              logEntryValueCaptor.getAllValues should have size 1
              logEntryIdCaptor.getAllValues should have size 1
              val actualLogEntryIdBytes = ByteString
                .copyFrom(logEntryIdCaptor.getValue.asInstanceOf[RawBytes])
              val expectedLogEntryIdBytes = ByteString.copyFrom(expectedLogEntryId.toByteArray)
              actualLogEntryIdBytes should be(expectedLogEntryIdBytes)
              ByteString
                .copyFrom(logEntryValueCaptor.getValue.asInstanceOf[RawBytes]) should not equal ByteString
                .copyFrom(logEntryIdCaptor.getValue.asInstanceOf[RawBytes])
          }
        }
    }

    "write marshalled key-value pairs to ledger" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      val expectedLogResult: Int = 7
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      val writtenKeyValuesCaptor = ArgumentCaptor.forClass(classOf[RawKeyValuePairs])
      when(mockStateOperations.writeState(writtenKeyValuesCaptor.capture()))
        .thenReturn(Future.successful(()))
      val logEntryCaptor = ArgumentCaptor.forClass(classOf[RawBytes])
      when(mockStateOperations.appendToLog(any[RawBytes](), logEntryCaptor.capture()))
        .thenReturn(Future.successful(expectedLogResult))
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates(1))
      val instance = new SubmissionValidator(
        new FakeStateAccess(mockStateOperations),
        (_, _, _, _, _) => logEntryAndStateResult,
        () => aLogEntryId())
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) {
            case Right(actualLogResult) =>
              actualLogResult should be(expectedLogResult)
              writtenKeyValuesCaptor.getAllValues should have size 1
              val writtenKeyValues = writtenKeyValuesCaptor.getValue.asInstanceOf[RawKeyValuePairs]
              writtenKeyValues should have size 1
              Try(SubmissionValidator.bytesToStateValue(writtenKeyValues.head._2)).isSuccess shouldBe true
              logEntryCaptor.getAllValues should have size 1
          }
        }
    }

    "return invalid submission if state cannot be written" in {
      val mockStateOperations = mock[LedgerStateOperations[Int]]
      when(mockStateOperations.writeState(any[RawKeyValuePairs]()))
        .thenThrow(new IllegalArgumentException("Write error"))
      when(mockStateOperations.readState(any[Seq[RawBytes]]()))
        .thenReturn(Future.successful(Seq(Some(aStateValue()))))
      when(mockStateOperations.appendToLog(any[RawBytes](), any[RawBytes]()))
        .thenReturn(Future.successful(99))
      val logEntryAndStateResult = (aLogEntry(), someStateUpdates(1))
      val instance = new SubmissionValidator(
        new FakeStateAccess(mockStateOperations),
        (_, _, _, _, _) => logEntryAndStateResult,
        () => aLogEntryId())
      instance
        .validateAndCommit(anEnvelope(), "aCorrelationId", newRecordTime(), aParticipantId())
        .map {
          inside(_) {
            case Left(ValidationError(reason)) => reason should include("Write error")
          }
        }
    }
  }

  private def aLogEntry(): DamlLogEntry =
    DamlLogEntry
      .newBuilder()
      .setPartyAllocationEntry(
        DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant"))
      .build()

  private def aLogEntryId(): DamlLogEntryId = SubmissionValidator.allocateRandomLogEntryId()

  private def someStateUpdates(count: Int): Map[DamlStateKey, DamlStateValue] =
    (1 to count).map { index =>
      val key = DamlStateKey
        .newBuilder()
        .setContractId(DamlContractId
          .newBuilder()
          .setEntryId(DamlLogEntryId.newBuilder.setEntryId(ByteString.copyFromUtf8(index.toString)))
          .setNodeId(index.toLong))
        .build
      val value = DamlStateValue.getDefaultInstance
      key -> value
    }.toMap

  private def aStateValue(): RawBytes =
    SubmissionValidator.valueToBytes(DamlStateValue.getDefaultInstance)

  private def anEnvelope(): RawBytes = {
    val submission = DamlSubmission
      .newBuilder()
      .setConfigurationSubmission(DamlConfigurationSubmission.getDefaultInstance)
      .addInputDamlState(DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance))
      .build
    Envelope.enclose(submission).toByteArray
  }

  private def aParticipantId(): ParticipantId = ParticipantId.assertFromString("aParticipantId")

  private def newRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def mockFunctionReturning[A](returnValue: A): () => A = {
    val mockFunction = mock[() => A]
    when(mockFunction.apply).thenReturn(returnValue)
    mockFunction
  }

  private class FakeStateAccess[LogResult](mockStateOperations: LedgerStateOperations[LogResult])
      extends LedgerStateAccess[LogResult] {
    override def inTransaction[T](body: LedgerStateOperations[LogResult] => Future[T]): Future[T] =
      body(mockStateOperations)
  }

}
