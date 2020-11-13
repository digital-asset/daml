// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._

/** Methods to produce the [[DamlSubmission]] message.
  *
  * [[DamlSubmission]] is processed for committing with [[KeyValueCommitting.processSubmission]].
  *
  * These methods are the only acceptable way of producing the submission messages.
  * The protocol buffer messages must not be embedded in other protocol buffer messages,
  * and embedding should happen through conversion into a byte string (via
  * [[KeyValueSubmission!.packDamlSubmission]]).
  */
class KeyValueSubmission(metrics: Metrics) {

  /** Given the assigned log entry id, compute the output state entries that would result
    * from committing the given transaction.
    *
    * Useful for implementations that require outputs to be known up-front.
    *
    * @deprecated Use [[KeyValueCommitting.submissionOutputs]] instead. This function will be removed in later version.
    */
  def transactionOutputs(tx: SubmittedTransaction): List[DamlStateKey] =
    metrics.daml.kvutils.submission.conversion.transactionOutputs.time { () =>
      val effects = InputsAndEffects.computeEffects(tx)
      effects.createdContracts.map(_._1) ++ effects.consumedContracts
    }

  /** Prepare a transaction submission. */
  def transactionToSubmission(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction,
  ): DamlSubmission =
    metrics.daml.kvutils.submission.conversion.transactionToSubmission.time { () =>
      val inputDamlStateFromTx = InputsAndEffects.computeInputs(tx, meta)
      val encodedSubInfo = buildSubmitterInfo(submitterInfo)

      DamlSubmission.newBuilder
        .addInputDamlState(commandDedupKey(encodedSubInfo))
        .addInputDamlState(configurationStateKey)
        .addInputDamlState(partyStateKey(submitterInfo.singleSubmitterOrThrow()))
        .addAllInputDamlState(inputDamlStateFromTx.asJava)
        .setTransactionEntry(
          DamlTransactionEntry.newBuilder
            .setTransaction(Conversions.encodeTransaction(tx))
            .setSubmitterInfo(encodedSubInfo)
            .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
            .setWorkflowId(meta.workflowId.getOrElse(""))
            .setSubmissionSeed(meta.submissionSeed.bytes.toByteString)
            .setSubmissionTime(buildTimestamp(meta.submissionTime))
        )
        .build
    }

  /** Prepare a package upload submission. */
  def archivesToSubmission(
      submissionId: String,
      archives: List[Archive],
      sourceDescription: String,
      participantId: ParticipantId,
  ): DamlSubmission =
    metrics.daml.kvutils.submission.conversion.archivesToSubmission.time { () =>
      val archivesDamlState =
        archives.map(
          archive =>
            DamlStateKey.newBuilder
              .setPackageId(archive.getHash)
              .build)

      DamlSubmission.newBuilder
        .addInputDamlState(packageUploadDedupKey(participantId, submissionId))
        .addAllInputDamlState(archivesDamlState.asJava)
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
      submissionId: SubmissionId,
      hint: Option[String],
      displayName: Option[String],
      participantId: ParticipantId,
  ): DamlSubmission =
    metrics.daml.kvutils.submission.conversion.partyToSubmission.time { () =>
      val party = hint.getOrElse("")
      DamlSubmission.newBuilder
        .addInputDamlState(partyAllocationDedupKey(participantId, submissionId))
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
      submissionId: SubmissionId,
      participantId: ParticipantId,
      config: Configuration,
  ): DamlSubmission =
    metrics.daml.kvutils.submission.conversion.configurationToSubmission.time { () =>
      val inputDamlState =
        configDedupKey(participantId, submissionId) ::
          configurationStateKey :: Nil
      DamlSubmission.newBuilder
        .addAllInputDamlState(inputDamlState.asJava)
        .setConfigurationSubmission(
          DamlConfigurationSubmission.newBuilder
            .setSubmissionId(submissionId)
            .setParticipantId(participantId)
            .setMaximumRecordTime(buildTimestamp(maxRecordTime))
            .setConfiguration(Configuration.encode(config))
        )
        .build
    }

  def packDamlSubmission(submission: DamlSubmission): ByteString = submission.toByteString

  def unpackDamlSubmission(bytes: ByteString): DamlSubmission = DamlSubmission.parseFrom(bytes)

}
