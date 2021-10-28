// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.daml_lf_dev.DamlLf
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
import com.daml.ledger.participant.state.kvutils.store.{
  DamlContractKey,
  DamlLogEntry,
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.{GlobalKey, TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

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
    inStaticTimeMode: Boolean,
) {
  import KeyValueCommitting.submissionOutputs

  private val logger = ContextualizedLogger.get(getClass)

  def this(engine: Engine, metrics: Metrics) = this(engine, metrics, false)

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
    * @param entryId: Log entry id to which this submission is committed.
    * @param recordTime: Record time at which this log entry is committed.
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
  def processSubmission(
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      defaultConfig: Configuration,
      submission: DamlSubmission,
      participantId: Ref.ParticipantId,
      inputState: DamlStateMap,
  )(implicit loggingContext: LoggingContext): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    metrics.daml.kvutils.committer.processing.inc()
    metrics.daml.kvutils.committer.last.lastRecordTimeGauge.updateValue(recordTime.toString)
    metrics.daml.kvutils.committer.last.lastEntryIdGauge.updateValue(Pretty.prettyEntryId(entryId))
    metrics.daml.kvutils.committer.last.lastParticipantIdGauge.updateValue(participantId)
    val ctx = metrics.daml.kvutils.committer.runTimer.time()
    try {
      val committer = createCommitter(engine, defaultConfig, submission)
      val (logEntry, outputState) = committer.run(
        Some(recordTime),
        submission,
        participantId,
        inputState,
      )
      verifyStateUpdatesAgainstPreDeclaredOutputs(outputState, submission)
      (logEntry, outputState)
    } catch {
      case scala.util.control.NonFatal(exception) =>
        logger.warn("Exception while processing submission.", exception)
        metrics.daml.kvutils.committer.last.lastExceptionGauge.updateValue(
          s"${Pretty.prettyEntryId(entryId)}[${submission.getPayloadCase}]: $exception"
        )
        throw exception
    } finally {
      val _ = ctx.stop()
      metrics.daml.kvutils.committer.processing.dec()
    }
  }

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
        new TransactionCommitter(defaultConfig, engine, metrics, inStaticTimeMode)

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidSubmission("DamlSubmission payload not set")
    }

  private def verifyStateUpdatesAgainstPreDeclaredOutputs(
      actualStateUpdates: Map[DamlStateKey, DamlStateValue],
      submission: DamlSubmission,
  ): Unit = {
    val expectedStateUpdates = submissionOutputs(submission)
    if (!(actualStateUpdates.keySet subsetOf expectedStateUpdates)) {
      val unaccountedKeys = actualStateUpdates.keySet diff expectedStateUpdates
      sys.error(
        s"State updates not a subset of expected updates! Keys [$unaccountedKeys] are unaccounted for!"
      )
    }
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
          archive: DamlLf.Archive =>
            DamlStateKey.newBuilder.setPackageId(archive.getHash).build
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
        transactionOutputs(transactionEntry) +
          commandDedupKey(transactionEntry.getSubmitterInfo)

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

  private def transactionOutputs(
      transactionEntry: DamlTransactionEntry
  ): Set[DamlStateKey] = {
    val outputs = Set.newBuilder[DamlStateKey]
    // TODO(KVL-1168): move to kv-transaction-support
    val transaction =
      TransactionOuterClass.Transaction.parseFrom(transactionEntry.getRawTransaction)
    val txVersion =
      TransactionCoder.decodeVersion(transaction.getVersion) match {
        case Right(value) => value
        case Left(err) => throw Err.DecodeError("Transaction", err.errorMessage)
      }

    transaction.getNodesList.asScala.foreach { node =>
      val nodeVersion = TransactionCoder.decodeNodeVersion(txVersion, node) match {
        case Right(value) => value
        case Left(err) => throw Err.DecodeError("Node", err.errorMessage)
      }

      node.getNodeTypeCase match {

        case TransactionOuterClass.Node.NodeTypeCase.ROLLBACK =>
        // Nodes under rollback will potentially produce outputs such as divulgence.
        // Actual outputs must be a subset of, or the same as, computed outputs and
        // we currently relax this check by widening the latter set, treating a node the
        // same regardless of whether it was under a rollback node or not.
        // Computed outputs that are not actual outputs can be safely trimmed: examples
        // are transient contracts and, now, potentially also outputs from nodes under
        // rollback.

        case TransactionOuterClass.Node.NodeTypeCase.CREATE =>
          val protoCreate = node.getCreate
          TransactionCoder.nodeKey(nodeVersion, protoCreate) match {
            case Right(Some(key)) =>
              outputs += contractKeyToStateKey(key)
            case Right(None) =>
            case Left(err) => throw Err.DecodeError("ContractKey", err.errorMessage)
          }
          outputs +=
            Conversions.contractIdStructOrStringToStateKey(protoCreate.getContractIdStruct)

        case TransactionOuterClass.Node.NodeTypeCase.EXERCISE =>
          val protoExercise = node.getExercise
          TransactionCoder.nodeKey(nodeVersion, protoExercise) match {
            case Right(Some(key)) =>
              outputs += contractKeyToStateKey(key)
            case Right(None) =>
            case Left(err) => throw Err.DecodeError("ContractKey", err.errorMessage)
          }
          outputs +=
            Conversions.contractIdStructOrStringToStateKey(protoExercise.getContractIdStruct)

        case TransactionOuterClass.Node.NodeTypeCase.FETCH =>
          // A fetch may cause a divulgence, which is why the target contract is a potential output.
          outputs +=
            Conversions.contractIdStructOrStringToStateKey(node.getFetch.getContractIdStruct)

        case TransactionOuterClass.Node.NodeTypeCase.LOOKUP_BY_KEY =>
        // Contract state only modified on divulgence, in which case we'll have a fetch node,
        // so no outputs from lookup node.

        case TransactionOuterClass.Node.NodeTypeCase.NODETYPE_NOT_SET =>
          throw Err.InvalidSubmission("submissionOutputs: NODETYPE_NOT_SET")
      }
    }
    outputs.result()
  }

  private def contractKeyToStateKey(
      key: GlobalKey
  ): DamlStateKey = {
    // NOTE(JM): The deserialization of the values is meant to be temporary. With the removal of relative
    // contract ids from kvutils submissions we will be able to up-front compute the outputs without having
    // to allocate a log entry id and we can directly place the output keys into the submission and do not need
    // to compute outputs from serialized transaction.
    DamlStateKey.newBuilder
      .setContractKey(
        DamlContractKey.newBuilder
          .setTemplateId(ValueCoder.encodeIdentifier(key.templateId))
          .setHash(key.hash.bytes.toByteString)
      )
      .build
  }
}
