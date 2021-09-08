// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.batch

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry.PayloadCase._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.lf.value.ValueCoder
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

import scala.annotation.nowarn

class ConflictDetection(val damlMetrics: Metrics) {
  private val logger = ContextualizedLogger.get(getClass)
  private val metrics = damlMetrics.daml.kvutils.conflictdetection

  /** Detect conflicts in a log entry and attempt to recover.
    * @param invalidatedKeys  set of keys that have been written up until now. We assume that a key
    *                         whose value hasn't been changed is not part of this set.
    * @param inputState input state used for the submission
    * @param logEntry log entry generated from processing the submission
    * @param outputState  output state generated from processing the submission
    * @param loggingContext logging context to use
    * @return pair of invalidated keys and output log entry and state; in case a conflict cannot be
    *         recovered None is output
    */
  def detectConflictsAndRecover(
      invalidatedKeys: collection.Set[DamlStateKey],
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
      logEntry: DamlLogEntry,
      outputState: Map[DamlStateKey, DamlStateValue],
  )(implicit
      loggingContext: LoggingContext
  ): Option[(collection.Set[DamlStateKey], (DamlLogEntry, Map[DamlStateKey, DamlStateValue]))] = {
    val newInvalidatedKeys = outputState.collect {
      case (key, outputValue)
          if changedStateValueOf(key, inputState.getOrElse(key, None), outputValue) =>
        key
    }.toSet
    val conflictingKeys = (inputState.keySet union outputState.keySet) intersect invalidatedKeys
    if (conflictingKeys.isEmpty) {
      // No conflicting keys, nothing to change.
      metrics.accepted.inc()
      Some((newInvalidatedKeys, (logEntry, outputState)))
    } else {
      // Conflicting keys.
      produceRejectionOrDropSubmission(logEntry, conflictingKeys)
        .map(logEntryAndState => newInvalidatedKeys -> logEntryAndState)
    }
  }

  private def changedStateValueOf(
      key: DamlStateKey,
      inputValueMaybe: Option[DamlStateValue],
      outputValue: DamlStateValue,
  )(implicit loggingContext: LoggingContext): Boolean =
    inputValueMaybe.forall { inputValue =>
      val contentsDiffer = inputValue.hashCode() != outputValue
        .hashCode() || inputValue != outputValue
      if (!contentsDiffer) {
        logger.trace(s"Dropped key=$key from conflicting key set as its value has not been altered")
        metrics.removedTransientKey.inc()
      }
      contentsDiffer
    }

  private def produceRejectionOrDropSubmission(
      logEntry: DamlLogEntry,
      conflictingKeys: Set[DamlStateKey],
  )(implicit
      loggingContext: LoggingContext
  ): Option[(DamlLogEntry, Map[DamlStateKey, DamlStateValue])] = {
    metrics.conflicted.inc()

    logEntry.getPayloadCase match {
      case TRANSACTION_ENTRY =>
        metrics.recovered.inc()
        val reason = explainConflict(conflictingKeys)
        val transactionRejectionEntry = transactionRejectionEntryFrom(logEntry, reason)
        logger.trace(s"Recovered a conflicted transaction, details='$reason'")
        Some((transactionRejectionEntry, Map.empty))

      case PARTY_ALLOCATION_ENTRY | PACKAGE_UPLOAD_ENTRY | CONFIGURATION_ENTRY |
          TRANSACTION_REJECTION_ENTRY | CONFIGURATION_REJECTION_ENTRY |
          PACKAGE_UPLOAD_REJECTION_ENTRY | PARTY_ALLOCATION_REJECTION_ENTRY |
          OUT_OF_TIME_BOUNDS_ENTRY | TIME_UPDATE_ENTRY =>
        logger.trace(s"Dropping conflicting submission (${logEntry.getPayloadCase})")
        metrics.dropped.inc()
        None

      case PAYLOAD_NOT_SET =>
        sys.error("detectConflictsAndRecover: PAYLOAD_NOT_SET")
    }
  }

  // Attempt to produce a useful message by collecting the first conflicting
  // contract id or contract key.
  private def explainConflict(conflictingKeys: Iterable[DamlStateKey]): String =
    conflictingKeys.toVector
      .sortBy(_.toByteString.asReadOnlyByteBuffer())
      .collectFirst {
        case key if key.hasContractKey =>
          // NOTE(JM): We show the template id as the other piece of data we have is the contract key
          // hash, which isn't very useful as it's an implementation detail not exposed over ledger-api.
          val templateId =
            ValueCoder
              .decodeIdentifier(key.getContractKey.getTemplateId)
              .map(_.toString)
              .getOrElse("<unknown>")
          s"Contract key conflicts in contract template $templateId"

        case key if key.getContractId.nonEmpty =>
          s"Conflict on contract ${key.getContractId}"

        case key if key.hasCommandDedup =>
          "Conflict on command deduplication, this command has already been processed."

        case key if key.hasConfiguration =>
          "Ledger configuration has changed"
      }
      .getOrElse("Unspecified conflict")

  @nowarn("msg=deprecated")
  private def transactionRejectionEntryFrom(
      logEntry: DamlLogEntry,
      reason: String,
  ): DamlLogEntry = {
    val builder = DamlLogEntry.newBuilder
    builder.setRecordTime(logEntry.getRecordTime)
    builder.getTransactionRejectionEntryBuilder
      .setSubmitterInfo(logEntry.getTransactionEntry.getSubmitterInfo)
      .getInconsistentBuilder
      .setDetails(reason)
    builder.build
  }
}
