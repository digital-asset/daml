// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.WriteSetComparison._
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, Raw}
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.PartialFunction.condOpt

final class RawWriteSetComparison(stateKeySerializationStrategy: StateKeySerializationStrategy)
    extends WriteSetComparison {

  override def compareWriteSets(
      expectedWriteSet: WriteSet,
      actualWriteSet: WriteSet,
  ): Option[String] =
    if (expectedWriteSet == actualWriteSet) {
      None
    } else {
      if (expectedWriteSet.size == actualWriteSet.size) {
        compareSameSizeWriteSets(expectedWriteSet, actualWriteSet)
      } else {
        Some(
          Seq(
            Seq(
              s"Expected write-set of size ${expectedWriteSet.size} vs. ${actualWriteSet.size}."
            ),
            Seq("Expected:"),
            writeSetToStrings(expectedWriteSet),
            Seq("Actual:"),
            writeSetToStrings(actualWriteSet),
          ).flatten.mkString(System.lineSeparator())
        )
      }
    }

  private def writeSetToStrings(writeSet: WriteSet): Seq[String] =
    writeSet.view.map((writeItemToString _).tupled).toVector

  private def writeItemToString(rawKey: Raw.Key, rawEnvelope: Raw.Envelope): String =
    parsePair(rawKey, rawEnvelope) match {
      case Left(errorMessage) =>
        throw new IllegalArgumentException(errorMessage)
      case Right(Left((logEntryId, logEntry))) =>
        s"$logEntryId -> $logEntry"
      case Right(Right((key, value))) =>
        s"$key -> $value"
    }

  private def compareSameSizeWriteSets(
      expectedWriteSet: WriteSet,
      actualWriteSet: WriteSet,
  ): Option[String] = {
    val differencesExplained = expectedWriteSet
      .zip(actualWriteSet)
      .map { case ((expectedKey, expectedValue), (actualKey, actualValue)) =>
        // We need to compare `.bytes` because the type will be different.
        // Unfortunately, the export format doesn't differentiate between log and state entries.
        // We don't want to change this because it will break backwards-compatibility.
        if (expectedKey.bytes == actualKey.bytes && expectedValue != actualValue) {
          Some(
            Seq(
              s"expected value:    ${rawHexString(expectedValue)}",
              s" vs. actual value: ${rawHexString(actualValue)}",
              explainDifference(expectedKey, expectedValue, actualValue),
            )
          )
        } else if (expectedKey.bytes != actualKey.bytes) {
          Some(
            Seq(
              s"expected key:    ${rawHexString(expectedKey)}",
              s" vs. actual key: ${rawHexString(actualKey)}",
            )
          )
        } else {
          None
        }
      }
      .map(_.toList)
      .filterNot(_.isEmpty)
      .flatten
      .flatten
      .mkString(System.lineSeparator())
    condOpt(differencesExplained.isEmpty) { case false =>
      differencesExplained
    }
  }

  private def explainDifference(
      key: Raw.Key,
      expectedValue: Raw.Envelope,
      actualValue: Raw.Envelope,
  ): String =
    kvutils.Envelope
      .openStateValue(expectedValue)
      .toOption
      .map { expectedStateValue =>
        val stateKey = stateKeySerializationStrategy.deserializeStateKey(Raw.StateKey(key.bytes))
        val actualStateValue = kvutils.Envelope.openStateValue(actualValue)
        s"""|State key: $stateKey
            |Expected: $expectedStateValue
            |Actual: $actualStateValue""".stripMargin
      }
      .getOrElse(explainMismatchingValue(key, expectedValue, actualValue))

  private def explainMismatchingValue(
      logEntryId: Raw.Key,
      expectedValue: Raw.Envelope,
      actualValue: Raw.Envelope,
  ): String = {
    val expectedLogEntry = kvutils.Envelope.openLogEntry(expectedValue)
    val actualLogEntry = kvutils.Envelope.openLogEntry(actualValue)
    s"Log entry ID: ${rawHexString(logEntryId)}${System.lineSeparator()}" +
      s"Expected: $expectedLogEntry${System.lineSeparator()}Actual: $actualLogEntry"
  }

  override def checkEntryIsReadable(
      rawKey: Raw.Key,
      rawEnvelope: Raw.Envelope,
  ): Either[String, Unit] =
    parsePair(rawKey, rawEnvelope).map(_ => ())

  private def parsePair(
      rawKey: Raw.Key,
      rawEnvelope: Raw.Envelope,
  ): Either[String, Either[
    (DamlKvutils.DamlLogEntryId, DamlKvutils.DamlLogEntry),
    (DamlStateKey, DamlStateValue),
  ]] =
    Envelope.open(rawEnvelope) match {
      case Left(errorMessage) =>
        Left(s"Invalid value envelope: $errorMessage")
      case Right(Envelope.LogEntryMessage(logEntry)) =>
        val logEntryId = DamlKvutils.DamlLogEntryId.parseFrom(rawKey.bytes)
        if (logEntry.getPayloadCase == DamlKvutils.DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET)
          Left("Log entry payload not set.")
        else
          Right(Left(logEntryId -> logEntry))
      case Right(Envelope.StateValueMessage(value)) =>
        val key = stateKeySerializationStrategy.deserializeStateKey(Raw.StateKey(rawKey.bytes))
        if (key.getKeyCase == DamlStateKey.KeyCase.KEY_NOT_SET)
          Left("State key not set.")
        else if (value.getValueCase == DamlStateValue.ValueCase.VALUE_NOT_SET)
          Left("State value not set.")
        else
          Right(Right(key -> value))
      case Right(Envelope.SubmissionMessage(submission)) =>
        Left(s"Unexpected submission message: $submission")
      case Right(Envelope.SubmissionBatchMessage(batch)) =>
        Left(s"Unexpected submission batch message: $batch")
    }

}
