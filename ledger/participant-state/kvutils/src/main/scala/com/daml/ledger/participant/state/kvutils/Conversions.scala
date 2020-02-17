// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{PackageId, SubmittedTransaction, SubmitterInfo}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.transaction.VersionTimeline.Implicits._
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  RelativeContractId,
  VersionedValue
}
import com.digitalasset.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.google.common.io.BaseEncoding
import com.google.protobuf.{ByteString, Empty}

/** Utilities for converting between protobuf messages and our scala
  * data structures.
  */
private[state] object Conversions {

  val configurationStateKey: DamlStateKey =
    DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build

  def partyStateKey(party: String): DamlStateKey =
    DamlStateKey.newBuilder.setParty(party).build

  def packageStateKey(packageId: PackageId): DamlStateKey =
    DamlStateKey.newBuilder.setPackageId(packageId).build

  def toAbsCoid(txId: DamlLogEntryId, coid: RelativeContractId): Ref.ContractIdString = {
    val hexTxId =
      BaseEncoding.base16.encode(txId.getEntryId.toByteArray)
    // NOTE(JM): Must be in sync with [[absoluteContractIdToLogEntryId]] and
    // [[absoluteContractIdToStateKey]].
    Ref.ContractIdString.assertFromString(s"$hexTxId:${coid.txnid.index}")
  }

  def absoluteContractIdToLogEntryId(acoid: AbsoluteContractId): (DamlLogEntryId, Int) =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        DamlLogEntryId.newBuilder
          .setEntryId(ByteString.copyFrom(BaseEncoding.base16.decode(hexTxId)))
          .build -> nodeId.toInt
      case _ =>
        throw Err.DecodeError("AbsoluteContractIdToLogEntryId", s"Cannot parse '${acoid.coid}'")
    }

  def absoluteContractIdToStateKey(acoid: AbsoluteContractId): DamlStateKey =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        DamlStateKey.newBuilder
          .setContractId(
            DamlContractId.newBuilder
              .setEntryId(
                DamlLogEntryId.newBuilder
                  .setEntryId(ByteString.copyFrom(BaseEncoding.base16.decode(hexTxId)))
                  .build)
              .setNodeId(nodeId.toLong)
              .build
          )
          .build
      case _ =>
        throw Err.DecodeError("AbsoluteContractIdToStakeKey", s"Cannot parse '${acoid.coid}'")
    }

  def relativeContractIdToStateKey(
      entryId: DamlLogEntryId,
      rcoid: RelativeContractId): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId(encodeRelativeContractId(entryId, rcoid))
      .build

  def encodeRelativeContractId(entryId: DamlLogEntryId, rcoid: RelativeContractId): DamlContractId =
    DamlContractId.newBuilder
      .setEntryId(entryId)
      .setNodeId(rcoid.txnid.index.toLong)
      .build

  def decodeContractId(coid: DamlContractId): AbsoluteContractId = {
    val hexTxId =
      BaseEncoding.base16.encode(coid.getEntryId.getEntryId.toByteArray)
    AbsoluteContractId(Ref.ContractIdString.assertFromString(s"$hexTxId:${coid.getNodeId}"))
  }

  def stateKeyToContractId(key: DamlStateKey): AbsoluteContractId = {
    decodeContractId(key.getContractId)
  }

  def encodeContractKey(key: GlobalKey): DamlContractKey = {
    val encodedValue = TransactionCoder
      .encodeValue(ValueCoder.CidEncoder, key.key)
      .getOrElse(throw Err.InternalError(s"contractKeyToStateKey: Cannot encode ${key.key}!"))
      ._2

    DamlContractKey.newBuilder
      .setTemplateId(ValueCoder.encodeIdentifier(key.templateId))
      .setKey(encodedValue)
      .build
  }

  def decodeContractKey(key: DamlContractKey): GlobalKey = {
    GlobalKey(
      ValueCoder
        .decodeIdentifier(key.getTemplateId)
        .getOrElse(
          throw Err
            .DecodeError("ContractKey", s"Cannot decode template id: ${key.getTemplateId}")
        ),
      forceNoContractIds(
        TransactionCoder
          .decodeValue(ValueCoder.CidDecoder, key.getKey)
          .fold(
            err =>
              throw Err
                .DecodeError("ContractKey", s"Cannot decode key: $err"),
            identity)
      )
    )
  }

  def contractKeyToStateKey(key: GlobalKey): DamlStateKey = {
    DamlStateKey.newBuilder
      .setContractKey(encodeContractKey(key))
      .build
  }

  def commandDedupKey(subInfo: DamlSubmitterInfo): DamlStateKey = {
    DamlStateKey.newBuilder
      .setCommandDedup(
        DamlCommandDedupKey.newBuilder
          .setApplicationId(subInfo.getApplicationId)
          .setCommandId(subInfo.getCommandId)
          .setSubmitter(subInfo.getSubmitter)
          .build
      )
      .build
  }

  def submissionDedupKey(
      participantId: String,
      submissionId: String,
      submissionKind: DamlSubmissionDedupKey.SubmissionKind): DamlStateKey = {
    DamlStateKey.newBuilder
      .setSubmissionDedup(
        DamlSubmissionDedupKey.newBuilder
          .setSubmissionKind(submissionKind)
          .setParticipantId(participantId)
          .setSubmissionId(submissionId)
          .build
      )
      .build
  }

  def packageUploadDedupKey(participantId: String, submissionId: String): DamlStateKey =
    submissionDedupKey(
      participantId,
      submissionId,
      DamlSubmissionDedupKey.SubmissionKind.PACKAGE_UPLOAD
    )

  def partyAllocationDedupKey(participantId: String, submissionId: String): DamlStateKey =
    submissionDedupKey(
      participantId,
      submissionId,
      DamlSubmissionDedupKey.SubmissionKind.PARTY_ALLOCATION
    )

  def configDedupKey(participantId: String, submissionId: String): DamlStateKey =
    submissionDedupKey(
      participantId,
      submissionId,
      DamlSubmissionDedupKey.SubmissionKind.CONFIGURATION
    )

  def buildSubmitterInfo(subInfo: SubmitterInfo): DamlSubmitterInfo =
    DamlSubmitterInfo.newBuilder
      .setSubmitter(subInfo.submitter)
      .setApplicationId(subInfo.applicationId)
      .setCommandId(subInfo.commandId)
      .setMaximumRecordTime(buildTimestamp(subInfo.maxRecordTime))
      .build

  def parseSubmitterInfo(subInfo: DamlSubmitterInfo): SubmitterInfo =
    SubmitterInfo(
      submitter = Party.assertFromString(subInfo.getSubmitter),
      applicationId = LedgerString.assertFromString(subInfo.getApplicationId),
      commandId = LedgerString.assertFromString(subInfo.getCommandId),
      maxRecordTime = parseTimestamp(subInfo.getMaximumRecordTime)
    )

  def buildTimestamp(ts: Time.Timestamp): com.google.protobuf.Timestamp = {
    val instant = ts.toInstant
    com.google.protobuf.Timestamp.newBuilder
      .setSeconds(instant.getEpochSecond)
      .setNanos(instant.getNano)
      .build
  }

  def parseTimestamp(ts: com.google.protobuf.Timestamp): Time.Timestamp =
    Time.Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.getSeconds, ts.getNanos.toLong))

  def buildDuration(dur: Duration): com.google.protobuf.Duration = {
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
  }

  def parseDuration(dur: com.google.protobuf.Duration): Duration = {
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)
  }

  def encodeTransaction(tx: SubmittedTransaction): TransactionOuterClass.Transaction = {
    TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      .fold(err => throw Err.EncodeError("Transaction", err.errorMessage), identity)
  }

  def decodeTransaction(tx: TransactionOuterClass.Transaction): SubmittedTransaction = {
    TransactionCoder
      .decodeVersionedTransaction(
        TransactionCoder.NidDecoder,
        ValueCoder.CidDecoder,
        tx
      )
      .fold(err => throw Err.DecodeError("Transaction", err.errorMessage), _.transaction)
  }

  def decodeContractInstance(coinst: TransactionOuterClass.ContractInstance)
    : Value.ContractInst[VersionedValue[AbsoluteContractId]] =
    TransactionCoder
      .decodeContractInstance(ValueCoder.CidDecoder, coinst)
      .fold(
        err => throw Err.DecodeError("ContractInstance", err.errorMessage),
        coinst =>
          coinst.ensureNoRelCid
            .fold(
              _ =>
                throw Err.InternalError(
                  "Relative contract identifier encountered in contract key!"),
              identity
          )
      )

  def encodeContractInstance(coinst: Value.ContractInst[VersionedValue[AbsoluteContractId]])
    : TransactionOuterClass.ContractInstance =
    TransactionCoder
      .encodeContractInstance(ValueCoder.CidEncoder, coinst)
      .fold(err => throw Err.InternalError(s"encodeContractInstance failed: $err"), identity)

  def forceNoContractIds(v: VersionedValue[ContractId]): VersionedValue[Nothing] =
    v.ensureNoCid.fold(
      coid => throw Err.InternalError(s"Contract identifier encountered in contract key! $coid"),
      identity,
    )

  def contractIdStructOrStringToStateKey[A](
      transactionVersion: TransactionVersion,
      entryId: DamlLogEntryId,
      coidString: String,
      coidStruct: ValueOuterClass.ContractId,
  ): DamlStateKey = {

    val result = ValueCoder.CidDecoder.decode(
      sv = transactionVersion,
      stringForm = coidString,
      structForm = coidStruct,
    )

    result match {
      case Left(err) =>
        throw Err.DecodeError("ContractId", s"Cannot decode contract id: $err")
      case Right(rcoid: RelativeContractId) =>
        relativeContractIdToStateKey(entryId, rcoid)
      case Right(acoid: AbsoluteContractId) =>
        absoluteContractIdToStateKey(acoid)
    }
  }

}
