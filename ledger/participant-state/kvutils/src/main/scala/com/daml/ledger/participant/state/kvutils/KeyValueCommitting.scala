// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.KeyValueCommitting.PreExecutionResult
import com.daml.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter
import com.daml.ledger.participant.state.kvutils.committer.{
  ConfigCommitter,
  PackageCommitter,
  PartyAllocationCommitter,
  SubmissionExecutor,
}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionEntry
import com.daml.ledger.participant.state.kvutils.store.{DamlLogEntry, DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.archive
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.kv.archives.{ArchiveConversions, RawArchive}
import com.daml.lf.kv.transactions.{ContractIdOrKey, RawTransaction, TransactionConversions}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._

// Added inStaticTimeMode to indicate whether the ledger uses static time mode or not.
// This has an impact on command deduplication and needs to be threaded through ProcessTransactionSubmission.
// See that class for more comments.
//
// The primary constructor is private to the daml package, because we don't expect any ledger other
// than sandbox to actually support static time.
class KeyValueCommitting private[daml] (
    engine: Engine,
    metrics: Metrics,
) {

  /** Processes a Daml submission, given the allocated log entry id, the submission and its resolved inputs.
    * Produces the log entry to be committed, and Daml state updates.
    *
    * The caller is expected to resolve the inputs declared in [[DamlSubmission]] prior
    * to calling this method, e.g. by reading [[DamlSubmission!.getInputEntriesList]] and
    * [[DamlSubmission!.getInputStateList]]
    *
    * The caller is expected to store the produced [[DamlLogEntry]] in key-value store at a location
    * that can be accessed through `entryId`. The Daml state updates may create new entries or update
    * existing entries in the key-value store.
    *
    * @param defaultConfig: The default configuration that is to be used if no configuration has been committed to state.
    * @param submission: Submission to commit to the ledger.
    * @param participantId: The participant from which the submission originates. Expected to be authenticated.
    * @param inputState:
    *   Resolved input state specified in submission. Optional to mark that input state was resolved
    *   but not present. Specifically we require the command de-duplication input to be resolved, but don't
    *   expect to be present.
    *   We also do not trust the submitter to provide the correct list of input keys and we need
    *   to verify that an input actually does not exist and was not just included in inputs.
    *   For example when committing a configuration we need the current configuration to authorize
    *   the submission.
    * @return Log entry to be committed and the Daml state updates to be applied.
    */
  @throws(classOf[Err])
  def preExecuteSubmission(
      defaultConfig: Configuration,
      submission: DamlSubmission,
      participantId: Ref.ParticipantId,
      inputState: DamlStateMap,
  )(implicit loggingContext: LoggingContext): PreExecutionResult =
    createCommitter(engine, defaultConfig, submission).runWithPreExecution(
      submission,
      participantId,
      inputState,
    )

  private def createCommitter(
      engine: Engine,
      defaultConfig: Configuration,
      submission: DamlSubmission,
  ): SubmissionExecutor =
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        new PackageCommitter(engine, metrics)

      case DamlSubmission.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        new PartyAllocationCommitter(metrics)

      case DamlSubmission.PayloadCase.CONFIGURATION_SUBMISSION =>
        val maximumRecordTime = parseTimestamp(
          submission.getConfigurationSubmission.getMaximumRecordTime
        )
        new ConfigCommitter(defaultConfig, maximumRecordTime, metrics)

      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        new TransactionCommitter(defaultConfig, engine, metrics)

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidSubmission("DamlSubmission payload not set")
    }

}

object KeyValueCommitting {
  case class PreExecutionResult(
      readSet: Set[DamlStateKey],
      successfulLogEntry: DamlLogEntry,
      stateUpdates: Map[DamlStateKey, DamlStateValue],
      outOfTimeBoundsLogEntry: DamlLogEntry,
      minimumRecordTime: Option[Timestamp],
      maximumRecordTime: Option[Timestamp],
  )

  /** Compute the submission outputs, that is the Daml State Keys created or updated by
    * the processing of the submission.
    */
  def submissionOutputs(submission: DamlSubmission): Set[DamlStateKey] = {
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        val packageEntry = submission.getPackageUploadEntry
        submission.getPackageUploadEntry.getArchivesList.asScala.toSet.map {
          rawArchive: ByteString =>
            ArchiveConversions.parsePackageId(RawArchive(rawArchive)) match {
              case Right(packageId) => DamlStateKey.newBuilder.setPackageId(packageId).build
              case Left(err: archive.Error) =>
                throw Err.InternalError(
                  s"${err.msg}: This should not happen, as the archives have just been validated."
                )
            }
        } + packageUploadDedupKey(packageEntry.getParticipantId, packageEntry.getSubmissionId)

      case DamlSubmission.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        val partyEntry = submission.getPartyAllocationEntry
        Set(
          DamlStateKey.newBuilder
            .setParty(submission.getPartyAllocationEntry.getParty)
            .build,
          partyAllocationDedupKey(partyEntry.getParticipantId, partyEntry.getSubmissionId),
        )

      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        val transactionEntry = submission.getTransactionEntry
        transactionOutputs(transactionEntry) + commandDedupKey(transactionEntry.getSubmitterInfo)

      case DamlSubmission.PayloadCase.CONFIGURATION_SUBMISSION =>
        val configEntry = submission.getConfigurationSubmission
        Set(
          configurationStateKey,
          configDedupKey(configEntry.getParticipantId, configEntry.getSubmissionId),
        )

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidSubmission("DamlSubmission payload not set")
    }
  }

  private def transactionOutputs(transactionEntry: DamlTransactionEntry): Set[DamlStateKey] =
    TransactionConversions
      .extractTransactionOutputs(RawTransaction(transactionEntry.getRawTransaction))
      .fold(
        err => throw Err.DecodeError("Transaction", err.errorMessage),
        _.map {
          case ContractIdOrKey.Id(id) => Conversions.contractIdToStateKey(id)
          case ContractIdOrKey.Key(key) => Conversions.globalKeyToStateKey(key)
        },
      )
}
