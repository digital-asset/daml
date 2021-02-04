// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.export.WriteSet
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.WriteSetComparison._
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, Raw}
import com.daml.ledger.validator.StateKeySerializationStrategy

import scala.PartialFunction.condOpt

class WriteSetComparison(stateKeySerializationStrategy: StateKeySerializationStrategy) {

  def compareWriteSets(expectedWriteSet: WriteSet, actualWriteSet: WriteSet): Option[String] =
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

  private def writeItemToString(key: Raw.Key, value: Raw.Value): String = {
    val keyString = stateKeySerializationStrategy.deserializeStateKey(key)
    val valueString = kvutils.Envelope.open(value)
    s"$keyString -> $valueString"
  }

  private[tools] def compareSameSizeWriteSets(
      expectedWriteSet: WriteSet,
      actualWriteSet: WriteSet,
  ): Option[String] = {
    val differencesExplained = expectedWriteSet
      .zip(actualWriteSet)
      .map { case ((expectedKey, expectedValue), (actualKey, actualValue)) =>
        if (expectedKey == actualKey && expectedValue != actualValue) {
          Some(
            Seq(
              s"expected value:    ${rawHexString(expectedValue)}",
              s" vs. actual value: ${rawHexString(actualValue)}",
              explainDifference(expectedKey, expectedValue, actualValue),
            )
          )
        } else if (expectedKey != actualKey) {
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
      expectedValue: Raw.Value,
      actualValue: Raw.Value,
  ): String =
    kvutils.Envelope
      .openStateValue(expectedValue)
      .toOption
      .map { expectedStateValue =>
        val stateKey = stateKeySerializationStrategy.deserializeStateKey(key)
        val actualStateValue = kvutils.Envelope.openStateValue(actualValue)
        s"""|State key: $stateKey
            |Expected: $expectedStateValue
            |Actual: $actualStateValue""".stripMargin
      }
      .getOrElse(explainMismatchingValue(key, expectedValue, actualValue))

  private def explainMismatchingValue(
      logEntryId: Raw.Key,
      expectedValue: Raw.Value,
      actualValue: Raw.Value,
  ): String = {
    val expectedLogEntry = kvutils.Envelope.openLogEntry(expectedValue)
    val actualLogEntry = kvutils.Envelope.openLogEntry(actualValue)
    s"Log entry ID: ${rawHexString(logEntryId)}${System.lineSeparator()}" +
      s"Expected: $expectedLogEntry${System.lineSeparator()}Actual: $actualLogEntry"
  }

  def checkEntryIsReadable(rawKey: Raw.Key, rawValue: Raw.Value): Either[String, Unit] =
    Envelope.open(rawValue) match {
      case Left(errorMessage) =>
        Left(s"Invalid value envelope: $errorMessage")
      case Right(Envelope.LogEntryMessage(logEntry)) =>
        val _ = DamlKvutils.DamlLogEntryId.parseFrom(rawKey.bytes)
        if (logEntry.getPayloadCase == DamlKvutils.DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET)
          Left("Log entry payload not set.")
        else
          Right(())
      case Right(Envelope.StateValueMessage(value)) =>
        val key = stateKeySerializationStrategy.deserializeStateKey(rawKey)
        if (key.getKeyCase == DamlKvutils.DamlStateKey.KeyCase.KEY_NOT_SET)
          Left("State key not set.")
        else if (value.getValueCase == DamlKvutils.DamlStateValue.ValueCase.VALUE_NOT_SET)
          Left("State value not set.")
        else
          Right(())
      case Right(Envelope.SubmissionMessage(submission)) =>
        Left(s"Unexpected submission message: $submission")
      case Right(Envelope.SubmissionBatchMessage(batch)) =>
        Left(s"Unexpected submission batch message: $batch")
    }

}

object WriteSetComparison {

  private[tools] def rawHexString(raw: Raw.Bytes): String =
    raw.bytes.toByteArray.map(byte => "%02x".format(byte)).mkString

}
