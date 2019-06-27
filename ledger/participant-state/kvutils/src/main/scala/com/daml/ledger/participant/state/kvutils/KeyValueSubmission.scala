// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationEntry,
  DamlPackageUploadEntry,
  DamlPartyAllocationEntry,
  DamlSubmission,
  DamlTimeModel,
  DamlTransactionEntry,
  DamlStateKey,
  DamlLogEntryId
}
import com.daml.ledger.participant.state.v1.{
  Configuration,
  SubmittedTransaction,
  SubmitterInfo,
  TransactionMeta
}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.google.protobuf.ByteString
import Conversions._

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
    */
  def transactionOutputs(entryId: DamlLogEntryId, tx: SubmittedTransaction): List[DamlStateKey] = {
    val effects = InputsAndEffects.computeEffects(entryId, tx)
    effects.createdContracts ++ effects.consumedContracts
  }

  /** Convert a transaction into a submission. */
  def transactionToSubmission(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction): DamlSubmission = {

    val (inputLogEntries, inputDamlStateFromTx) = InputsAndEffects.computeInputs(tx)
    val encodedSubInfo = buildSubmitterInfo(submitterInfo)
    val inputDamlState = commandDedupKey(encodedSubInfo) :: inputDamlStateFromTx

    DamlSubmission.newBuilder
      .addAllInputLogEntries(inputLogEntries.asJava)
      .addAllInputDamlState(inputDamlState.asJava)
      .setTransactionEntry(
        DamlTransactionEntry.newBuilder
          .setTransaction(Conversions.encodeTransaction(tx))
          .setSubmitterInfo(encodedSubInfo)
          .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
          .setWorkflowId(meta.workflowId.getOrElse(""))
          .build
      )
      .build
  }

  /** Convert an archive into a submission message. */
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
          .build
      )
      .build
  }

  /** Convert an archive into a submission message. */
  def partyToSubmission(
      submissionId: String,
      hint: Option[String],
      displayName: Option[String],
      participantId: String): DamlSubmission = {
    val party = hint.getOrElse("")
    val inputDamlState = List(
      DamlStateKey.newBuilder
        .setParty(party)
        .build)
    DamlSubmission.newBuilder
      .addAllInputDamlState(inputDamlState.asJava)
      .setPartyAllocationEntry(
        DamlPartyAllocationEntry.newBuilder
          .setSubmissionId(submissionId)
          .setParty(party)
          .setParticipantId(participantId)
          .setDisplayName(displayName.getOrElse(""))
          .build
      )
      .build
  }

  /** Convert ledger configuratino into a submission message. */
  def configurationToSubmission(config: Configuration): DamlSubmission = {
    val tm = config.timeModel
    DamlSubmission.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setTimeModel(
            DamlTimeModel.newBuilder
              .setMaxClockSkew(buildDuration(tm.maxClockSkew))
              .setMinTransactionLatency(buildDuration(tm.minTransactionLatency))
              .setMaxTtl(buildDuration(tm.maxTtl))
          )
      )
      .build
  }

  def packDamlSubmission(submission: DamlSubmission): ByteString = submission.toByteString
  def unpackDamlSubmission(bytes: ByteString): DamlSubmission = DamlSubmission.parseFrom(bytes)

}
