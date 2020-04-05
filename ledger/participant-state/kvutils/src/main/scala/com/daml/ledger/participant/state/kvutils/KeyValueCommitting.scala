// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.codahale.metrics
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.{
  ConfigCommitter,
  PackageCommitter,
  PartyAllocationCommitter
}
import com.daml.ledger.participant.state.kvutils.committing._
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.Engine
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueOuterClass
import com.daml.daml_lf_dev.DamlLf
import com.daml.platform.common.metrics.VarGauge
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

// Added inStaticTimeMode to indicate whether the ledger uses static time mode or not.
// This has an impact on command deduplication and needs to be threaded through ProcessTransactionSubmission.
// See that class for more comments.
//
// The primary constructor is private to the daml package, because we don't expect any ledger other
// than sandbox to actually support static time.
class KeyValueCommitting private[daml] (metricRegistry: MetricRegistry, inStaticTimeMode: Boolean) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def this(metricRegistry: MetricRegistry) = this(metricRegistry, false)

  def packDamlStateKey(key: DamlStateKey): ByteString = key.toByteString

  def unpackDamlStateKey(bytes: ByteString): DamlStateKey =
    DamlStateKey.parseFrom(bytes)

  def packDamlStateValue(value: DamlStateValue): ByteString = value.toByteString

  def unpackDamlStateValue(bytes: Array[Byte]): DamlStateValue =
    DamlStateValue.parseFrom(bytes)

  def unpackDamlStateValue(bytes: ByteString): DamlStateValue =
    DamlStateValue.parseFrom(bytes)

  def packDamlLogEntry(entry: DamlLogEntry): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry =
    DamlLogEntry.parseFrom(bytes)

  def packDamlLogEntryId(entry: DamlLogEntryId): ByteString = entry.toByteString
  def unpackDamlLogEntryId(bytes: Array[Byte]): DamlLogEntryId =
    DamlLogEntryId.parseFrom(bytes)
  def unpackDamlLogEntryId(bytes: ByteString): DamlLogEntryId =
    DamlLogEntryId.parseFrom(bytes)

  // A stop-gap measure, to be used while maximum record time is not yet available on every request
  private def estimateMaximumRecordTime(recordTime: Timestamp): Timestamp =
    recordTime.addMicros(100)

  /** Processes a DAML submission, given the allocated log entry id, the submission and its resolved inputs.
    * Produces the log entry to be committed, and DAML state updates.
    *
    * The caller is expected to resolve the inputs declared in [[DamlSubmission]] prior
    * to calling this method, e.g. by reading [[DamlSubmission!.getInputEntriesList]] and
    * [[DamlSubmission!.getInputStateList]]
    *
    * The caller is expected to store the produced [[DamlLogEntry]] in key-value store at a location
    * that can be accessed through `entryId`. The DAML state updates may create new entries or update
    * existing entries in the key-value store. The concrete key for DAML state entry is obtained by applying
    * [[packDamlStateKey]] to [[DamlStateKey]].
    *
    * @param engine: Engine instance to use for interpreting submission.
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
    * @return Log entry to be committed and the DAML state updates to be applied.
    */
  @throws(classOf[Err])
  def processSubmission(
      engine: Engine,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      defaultConfig: Configuration,
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    Metrics.processing.inc()
    Metrics.lastRecordTimeGauge.updateValue(recordTime.toString)
    Metrics.lastEntryIdGauge.updateValue(Pretty.prettyEntryId(entryId))
    Metrics.lastParticipantIdGauge.updateValue(participantId)
    val ctx = Metrics.runTimer.time()
    try {
      val (logEntry, outputState) = processPayload(
        engine,
        entryId,
        recordTime,
        defaultConfig,
        submission,
        participantId,
        inputState,
      )
      Debug.dumpLedgerEntry(submission, participantId, entryId, logEntry, outputState)
      verifyStateUpdatesAgainstPreDeclaredOutputs(outputState, entryId, submission)
      (logEntry, outputState)
    } catch {
      case scala.util.control.NonFatal(e) =>
        logger.warn(s"Exception while processing submission, error='$e'")
        Metrics.lastExceptionGauge.updateValue(
          Pretty
            .prettyEntryId(entryId) + s"[${submission.getPayloadCase}]: " + e.toString,
        )
        throw e
    } finally {
      val _ = ctx.stop()
      Metrics.processing.dec()
    }
  }

  private def processPayload(
      engine: Engine,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      defaultConfig: Configuration,
      submission: DamlSubmission,
      participantId: ParticipantId,
      inputState: Map[DamlStateKey, Option[DamlStateValue]],
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        new PackageCommitter(engine, metricRegistry).run(
          entryId,
          //TODO replace this call with an explicit maxRecordTime from the request once available
          estimateMaximumRecordTime(recordTime),
          recordTime,
          submission.getPackageUploadEntry,
          participantId,
          inputState,
        )

      case DamlSubmission.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        new PartyAllocationCommitter(metricRegistry).run(
          entryId,
          //TODO replace this call with an explicit maxRecordTime from the request once available
          estimateMaximumRecordTime(recordTime),
          recordTime,
          submission.getPartyAllocationEntry,
          participantId,
          inputState,
        )

      case DamlSubmission.PayloadCase.CONFIGURATION_SUBMISSION =>
        new ConfigCommitter(defaultConfig, metricRegistry).run(
          entryId,
          parseTimestamp(submission.getConfigurationSubmission.getMaximumRecordTime),
          recordTime,
          submission.getConfigurationSubmission,
          participantId,
          inputState,
        )

      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        new ProcessTransactionSubmission(defaultConfig, engine, metricRegistry, inStaticTimeMode)
          .run(
            entryId,
            recordTime,
            participantId,
            submission.getTransactionEntry,
            inputState,
          )

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidSubmission("DamlSubmission payload not set")
    }

  /** Compute the submission outputs, that is the DAML State Keys created or updated by
    * the processing of the submission.
    */
  def submissionOutputs(entryId: DamlLogEntryId, submission: DamlSubmission): Set[DamlStateKey] = {
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
        transactionOutputs(transactionEntry, entryId) + commandDedupKey(
          transactionEntry.getSubmitterInfo,
        )

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
      transactionEntry: DamlTransactionEntry,
      entryId: DamlLogEntryId,
  ): Set[DamlStateKey] = {
    val transaction = transactionEntry.getTransaction
    val transactionVersion = TransactionCoder
      .decodeVersion(transaction.getVersion)
      .fold(err => throw Err.InvalidSubmission(err.errorMessage), identity)
    transaction.getNodesList.asScala.flatMap { node: TransactionOuterClass.Node =>
      node.getNodeTypeCase match {
        case TransactionOuterClass.Node.NodeTypeCase.CREATE =>
          val create = node.getCreate
          val templateId = create.getContractInstance.getTemplateId
          val contractKeyStateKeyOrEmpty =
            if (create.hasKeyWithMaintainers)
              List(contractKeyToStateKey(templateId, create.getKeyWithMaintainers.getKey))
            else
              List.empty

          Conversions
            .contractIdStructOrStringToStateKey(
              transactionVersion,
              entryId,
              create.getContractId,
              create.getContractIdStruct,
            ) :: contractKeyStateKeyOrEmpty

        case TransactionOuterClass.Node.NodeTypeCase.EXERCISE =>
          val exe = node.getExercise
          val contractKeyStateKeyOrEmpty =
            if (exe.getConsuming && exe.hasKeyWithMaintainers)
              List(contractKeyToStateKey(exe.getTemplateId, exe.getKeyWithMaintainers.getKey))
            else
              List.empty

          Conversions
            .contractIdStructOrStringToStateKey(
              transactionVersion,
              entryId,
              exe.getContractId,
              exe.getContractIdStruct,
            ) :: contractKeyStateKeyOrEmpty

        case TransactionOuterClass.Node.NodeTypeCase.FETCH =>
          // A fetch may cause a divulgence, which is why the target contract is a potential output.
          List(
            Conversions
              .contractIdStructOrStringToStateKey(
                transactionVersion,
                entryId,
                node.getFetch.getContractId,
                node.getFetch.getContractIdStruct,
              ),
          )
        case TransactionOuterClass.Node.NodeTypeCase.LOOKUP_BY_KEY =>
          // Contract state only modified on divulgence, in which case we'll have a fetch node,
          // so no outputs from lookup node.
          List.empty

        case TransactionOuterClass.Node.NodeTypeCase.NODETYPE_NOT_SET =>
          throw Err.InvalidSubmission("submissionOutputs: NODETYPE_NOT_SET")
      }
    }.toSet
  }

  private def contractKeyToStateKey(
      templateId: ValueOuterClass.Identifier,
      key: ValueOuterClass.VersionedValue): DamlStateKey = {
    // NOTE(JM): The deserialization of the values is meant to be temporary. With the removal of relative
    // contract ids from kvutils submissions we will be able to up-front compute the outputs without having
    // to allocate a log entry id and we can directly place the output keys into the submission and do not need
    // to compute outputs from serialized transaction.
    val contractKey =
      GlobalKey(
        decodeIdentifier(templateId),
        decodeVersionedValue(key).value.ensureNoCid
          .getOrElse(throw Err.DecodeError("ContractKey", s"Contract key contained contract id"))
      )
    DamlStateKey.newBuilder
      .setContractKey(
        DamlContractKey.newBuilder
          .setTemplateId(templateId)
          .setHash(contractKey.hash.bytes.toByteString))
      .build
  }

  private def verifyStateUpdatesAgainstPreDeclaredOutputs(
      actualStateUpdates: Map[DamlStateKey, DamlStateValue],
      entryId: DamlLogEntryId,
      submission: DamlSubmission,
  ): Unit = {
    val expectedStateUpdates = submissionOutputs(entryId, submission)
    if (!(actualStateUpdates.keySet subsetOf expectedStateUpdates)) {
      val unaccountedKeys = actualStateUpdates.keySet diff expectedStateUpdates
      sys.error(
        s"State updates not a subset of expected updates! Keys [$unaccountedKeys] are unaccounted for!",
      )
    }
  }

  private object Metrics {
    private val prefix = MetricPrefix :+ "committer"
    private val lastPrefix = prefix :+ "last"

    // Timer (and count) of how fast submissions have been processed.
    val runTimer: metrics.Timer = metricRegistry.timer(prefix :+ "run_timer")

    // Number of exceptions seen.
    val exceptions: metrics.Counter = metricRegistry.counter(prefix :+ "exceptions")

    // Counter to monitor how many at a time and when kvutils is processing a submission.
    val processing: metrics.Counter = metricRegistry.counter(prefix :+ "processing")

    val lastRecordTimeGauge = new VarGauge[String]("<none>")
    metricRegistry.register(lastPrefix :+ "record_time", lastRecordTimeGauge)

    val lastEntryIdGauge = new VarGauge[String]("<none>")
    metricRegistry.register(lastPrefix :+ "entry_id", lastEntryIdGauge)

    val lastParticipantIdGauge = new VarGauge[String]("<none>")
    metricRegistry.register(lastPrefix :+ "participant_id", lastParticipantIdGauge)

    val lastExceptionGauge = new VarGauge[String]("<none>")
    metricRegistry.register(lastPrefix :+ "exception", lastExceptionGauge)
  }

}
