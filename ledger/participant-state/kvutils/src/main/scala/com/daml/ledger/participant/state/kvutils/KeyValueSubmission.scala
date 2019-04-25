package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationEntry,
  DamlSubmission,
  DamlTimeModel,
  DamlTransactionEntry
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
  * These methods are the only acceptable way of producing the submission messages.
  *
  * The protocol buffer messages must not be embedded in other protocol buffer messages,
  * and embedding should happen through conversion into a byte string (via [[KeyValueSubmission.packDamlSubmission]])
  */
object KeyValueSubmission {

  def transactionToSubmission(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction): DamlSubmission = {

    val (inputLogEntries, inputDamlState) = InputsAndEffects.computeInputs(tx)

    DamlSubmission.newBuilder
      .addAllInputLogEntries(inputLogEntries.asJava)
      .addAllInputDamlState(inputDamlState.asJava)
      .setTransactionEntry(
        DamlTransactionEntry.newBuilder
          .setTransaction(Conversions.encodeTransaction(tx))
          .setSubmitterInfo(buildSubmitterInfo(submitterInfo))
          .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
          .setWorkflowId(meta.workflowId)
          .build
      )
      .build
  }

  def archiveToSubmission(archive: Archive): DamlSubmission = {
    DamlSubmission.newBuilder
      .setArchive(archive)
      .build
  }

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
