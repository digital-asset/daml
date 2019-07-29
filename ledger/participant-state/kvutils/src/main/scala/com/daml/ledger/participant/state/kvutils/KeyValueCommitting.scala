// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committing.{
  ProcessPackageUpload,
  ProcessPartyAllocation,
  ProcessTransactionSubmission
}
import com.daml.ledger.participant.state.v1.Configuration
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

object KeyValueCommitting {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Errors that can result from improper calls to processSubmission.
    * Validation and consistency errors are turned into command rejections.
    * Note that processSubmission can also fail with a protobuf exception,
    * e.g. https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/InvalidProtocolBufferException.
    */
  sealed trait Err extends RuntimeException with Product with Serializable
  object Err {
    final case class InvalidPayload(message: String) extends Err
    final case class MissingInputLogEntry(entryId: DamlLogEntryId) extends Err
    final case class MissingInputState(key: DamlStateKey) extends Err
    final case class NodeMissingFromLogEntry(entryId: DamlLogEntryId, nodeId: Int) extends Err
    final case class NodeNotACreate(entryId: DamlLogEntryId, nodeId: Int) extends Err
    final case class ArchiveDecodingFailed(packageId: PackageId, reason: String) extends Err
    final case class InternalError(message: String) extends Err
  }

  def packDamlStateKey(key: DamlStateKey): ByteString = key.toByteString
  def unpackDamlStateKey(bytes: ByteString): DamlStateKey = DamlStateKey.parseFrom(bytes)

  def packDamlStateValue(value: DamlStateValue): ByteString = value.toByteString
  def unpackDamlStateValue(bytes: ByteString): DamlStateValue = DamlStateValue.parseFrom(bytes)

  def packDamlLogEntry(entry: DamlLogEntry): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry = DamlLogEntry.parseFrom(bytes)

  def packDamlLogEntryId(entry: DamlLogEntryId): ByteString = entry.toByteString
  def unpackDamlLogEntryId(bytes: ByteString): DamlLogEntryId = DamlLogEntryId.parseFrom(bytes)

  /** Pretty-printing of the entry identifier. Uses the same hexadecimal encoding as is used
    * for absolute contract identifiers.
    */
  def prettyEntryId(entryId: DamlLogEntryId): String =
    BaseEncoding.base16.encode(entryId.getEntryId.toByteArray)

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
    * @param engine: DAML Engine. This instance should be persistent as it caches package compilation.
    * @param config: Ledger configuration.
    * @param entryId: Log entry id to which this submission is committed.
    * @param recordTime: Record time at which this log entry is committed.
    * @param submission: Submission to commit to the ledger.
    * @param inputLogEntries: Resolved input log entries specified in submission.
    * @param inputState:
    *   Resolved input state specified in submission. Optional to mark that input state was resolved
    *   but not present. Specifically we require the command de-duplication input to be resolved, but don't
    *   expect to be present.
    * @return Log entry to be committed and the DAML state updates to be applied.
    */
  def processSubmission(
      engine: Engine,
      config: Configuration,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      submission: DamlSubmission,
      inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
      inputState: Map[DamlStateKey, Option[DamlStateValue]]
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {

    // Look at what kind of submission this is...
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.PACKAGE_UPLOAD_ENTRY =>
        ProcessPackageUpload(
          entryId,
          recordTime,
          submission.getPackageUploadEntry,
          inputState
        ).run

      case DamlSubmission.PayloadCase.PARTY_ALLOCATION_ENTRY =>
        ProcessPartyAllocation(
          entryId,
          recordTime,
          submission.getPartyAllocationEntry,
          inputState
        ).run

      case DamlSubmission.PayloadCase.CONFIGURATION_ENTRY =>
        logger.trace(
          s"processSubmission[entryId=${prettyEntryId(entryId)}]: New configuration committed.")
        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setConfigurationEntry(submission.getConfigurationEntry)
            .build,
          Map.empty
        )

      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        ProcessTransactionSubmission(
          engine,
          config,
          entryId,
          recordTime,
          submission.getTransactionEntry,
          inputLogEntries,
          inputState
        ).run

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidPayload("DamlSubmission.payload not set.")
    }
  }

}
