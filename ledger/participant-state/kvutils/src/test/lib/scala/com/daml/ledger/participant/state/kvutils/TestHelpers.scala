// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration
import java.util.UUID

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlSubmitterInfo,
  DamlTransactionEntry,
  DamlTransactionRejectionEntry,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.DamlTransactionEntrySummary
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepResult, StepStop}
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.transaction.test.TransactionBuilder
import com.daml.lf.value.Value
import com.google.protobuf.ByteString

import scala.jdk.CollectionConverters._

object TestHelpers {
  def name(value: String): Ref.Name = Ref.Name.assertFromString(value)

  def party(value: String): Ref.Party = Ref.Party.assertFromString(value)

  val badArchive: DamlLf.Archive =
    DamlLf.Archive.newBuilder
      .setHash("blablabla")
      .build

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig: Configuration = Configuration(
    generation = 0,
    timeModel = LedgerTimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  def mkEntryId(n: Int): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build

  def mkParticipantId(n: Int): Ref.ParticipantId =
    Ref.ParticipantId.assertFromString(s"participant-$n")

  def randomLedgerString: Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  def createCommitContext(
      recordTime: Option[Timestamp],
      inputs: DamlStateMap = Map.empty,
      participantId: Int = 0,
  ): CommitContext =
    CommitContext(inputs, recordTime, mkParticipantId(participantId))

  def createEmptyTransactionEntry(submitters: List[String]): DamlTransactionEntry =
    createTransactionEntry(submitters, TransactionBuilder.EmptySubmitted)

  def createTransactionEntry(
      submitters: List[String],
      tx: SubmittedTransaction,
  ): DamlTransactionEntry =
    DamlTransactionEntry.newBuilder
      .setTransaction(Conversions.encodeTransaction(tx))
      .setSubmitterInfo(
        DamlSubmitterInfo.newBuilder
          .setCommandId("commandId")
          .addAllSubmitters(submitters.asJava)
      )
      .setSubmissionSeed(ByteString.copyFromUtf8("a" * 32))
      .build

  def getTransactionRejectionReason(
      result: StepResult[DamlTransactionEntrySummary]
  ): DamlTransactionRejectionEntry =
    result
      .asInstanceOf[StepStop]
      .logEntry
      .getTransactionRejectionEntry

  def lfTuple(values: String*): Value =
    TransactionBuilder.record(values.zipWithIndex.map { case (v, i) =>
      s"_$i" -> v
    }: _*)
}
