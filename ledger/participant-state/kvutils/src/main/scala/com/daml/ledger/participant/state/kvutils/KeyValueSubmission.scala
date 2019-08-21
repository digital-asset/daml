// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta
}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.google.protobuf.ByteString
import Conversions._
import com.digitalasset.daml.lf.data.Time.Timestamp

import scala.collection.JavaConverters._

/** Methods to produce the [[DamlSubmission]] message.
  * [[DamlSubmission]] is processed for committing with [[KeyValueCommitting.processSubmission]].
  *
  * These methods are the only acceptable way of producing the submission messages.
  * The protocol buffer messages must not be embedded in other protocol buffer messages,
  * and embedding should happen through conversion into a byte string (via [[KeyValueSubmission.packDamlSubmission]])
  */
object KeyValueSubmission {

  /** Given the assigned log entry id, compute the output state entries that would result
    * from committing the given transaction.
    *
    * Useful for implementations that require outputs to be known up-front.
    *
    * @deprecated Use [[KeyValueCommitting.submissionOutputs]] instead. This function will be removed in later version.
    */
  def transactionOutputs(entryId: DamlLogEntryId, tx: SubmittedTransaction): List[DamlStateKey] = {
    val effects = InputsAndEffects.computeEffects(entryId, tx)
    effects.createdContracts.map(_._1) ++ effects.consumedContracts
  }

  /** Prepare a transaction submission. */
  def transactionToSubmission(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction): DamlSubmission = {

    val inputDamlStateFromTx = InputsAndEffects.computeInputs(tx)
    val encodedSubInfo = buildSubmitterInfo(submitterInfo)
    val inputDamlState =
      commandDedupKey(encodedSubInfo) ::
        configurationStateKey ::
        partyStateKey(submitterInfo.submitter) ::
        inputDamlStateFromTx

    DamlSubmission.newBuilder
      .addAllInputDamlState(inputDamlState.asJava)
      .setTransactionEntry(
        DamlTransactionEntry.newBuilder
          .setTransaction(Conversions.encodeTransaction(tx))
          .setSubmitterInfo(encodedSubInfo)
          .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
          .setWorkflowId(meta.workflowId.getOrElse(""))
      )
      .build
  }

  /** Prepare a package upload submission. */
  def archivesToSubmission(
      submissionId: String,
      archives: List[Archive],
      sourceDescription: String,
      participantId: String): DamlSubmission = {

    val inputDamlState = archives.map(
      archive =>
        DamlStateKey.newBuilder
          .setPackageId(archive.getHash)
          .build)

    DamlSubmission.newBuilder
      .addAllInputDamlState(inputDamlState.asJava)
      .setPackageUploadEntry(
        DamlPackageUploadEntry.newBuilder
          .setSubmissionId(submissionId)
          .addAllArchives(archives.asJava)
          .setSourceDescription(sourceDescription)
          .setParticipantId(participantId)
      )
      .build
  }

  /** Prepare a party allocation submission. */
  def partyToSubmission(
      submissionId: String,
      hint: Option[String],
      displayName: Option[String],
      participantId: String): DamlSubmission = {
    val party = hint.getOrElse("")
    DamlSubmission.newBuilder
      .addInputDamlState(partyStateKey(party))
      .setPartyAllocationEntry(
        DamlPartyAllocationEntry.newBuilder
          .setSubmissionId(submissionId)
          .setParty(party)
          .setParticipantId(participantId)
          .setDisplayName(displayName.getOrElse(""))
      )
      .build
  }

  /** Prepare a ledger configuration change submission. */
  def configurationToSubmission(
      maxRecordTime: Timestamp,
      submissionId: String,
      config: Configuration): DamlSubmission = {
    val tm = config.timeModel
    DamlSubmission.newBuilder
      .addInputDamlState(configurationStateKey)
      .setConfigurationSubmission(
        DamlConfigurationSubmission.newBuilder
          .setSubmissionId(submissionId)
          .setMaximumRecordTime(buildTimestamp(maxRecordTime))
          .setConfiguration(buildDamlConfiguration(config))
      )
      .build
  }

  def packDamlSubmission(submission: DamlSubmission): ByteString = submission.toByteString
  def unpackDamlSubmission(bytes: ByteString): DamlSubmission = DamlSubmission.parseFrom(bytes)

}
