// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{PackageId, SubmitterInfo}
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Identifier, LedgerString, Party}
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

  def contractIdToStateKey(acoid: AbsoluteContractId): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId(acoid.coid)
      .build

  def decodeContractId(coid: String): AbsoluteContractId =
    AbsoluteContractId(Ref.ContractIdString.assertFromString(coid))

  def stateKeyToContractId(key: DamlStateKey): AbsoluteContractId =
    decodeContractId(key.getContractId)

  def encodeGlobalKey(key: GlobalKey): DamlContractKey = {
    DamlContractKey.newBuilder
      .setTemplateId(ValueCoder.encodeIdentifier(key.templateId))
      .setHash(ByteString.copyFrom(key.hash.toByteArray))
      .build
  }

  def decodeIdentifier(protoIdent: ValueOuterClass.Identifier): Identifier =
    ValueCoder
      .decodeIdentifier(protoIdent)
      .getOrElse(
        throw Err
          .DecodeError("Identifier", s"Cannot decode identifier: $protoIdent"))

  def globalKeyToStateKey(key: GlobalKey): DamlStateKey = {
    DamlStateKey.newBuilder
      .setContractKey(encodeGlobalKey(key))
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

  def parseHash(a: com.google.protobuf.ByteString): crypto.Hash =
    crypto.Hash.assertFromBytes(a.toByteArray)

  def parseOptHash(a: com.google.protobuf.ByteString): Option[crypto.Hash] =
    if (a.isEmpty)
      None
    else
      Some(crypto.Hash.assertFromBytes(a.toByteArray))

  def buildDuration(dur: Duration): com.google.protobuf.Duration = {
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
  }

  def parseDuration(dur: com.google.protobuf.Duration): Duration = {
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)
  }

  def encodeTransaction(tx: Transaction.AbsTransaction): TransactionOuterClass.Transaction = {
    TransactionCoder
      .encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx)
      .fold(err => throw Err.EncodeError("Transaction", err.errorMessage), identity)
  }

  def decodeTransaction(tx: TransactionOuterClass.Transaction): Transaction.AbsTransaction = {
    TransactionCoder
      .decodeVersionedTransaction(
        TransactionCoder.NidDecoder,
        ValueCoder.AbsCidDecoder,
        tx
      )
      .fold(err => throw Err.DecodeError("Transaction", err.errorMessage), _.transaction)
  }

  def decodeVersionedValue(
      protoValue: ValueOuterClass.VersionedValue): VersionedValue[AbsoluteContractId] =
    ValueCoder
      .decodeVersionedValue(ValueCoder.CidDecoder, protoValue)
      .fold(
        err => throw Err.DecodeError("ContractInstance", err.errorMessage),
        value =>
          value.ensureNoRelCid
            .fold(
              _ =>
                throw Err.InternalError(
                  "Relative contract identifier encountered in contract key!"),
              identity
          )
      )

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

  def forceNoContractIds(v: Value[ContractId]): Value[Nothing] =
    v.ensureNoCid.fold(
      coid => throw Err.InternalError(s"Contract identifier '$coid' encountered in contract key"),
      identity,
    )

  def contractIdStructOrStringToStateKey[A](
      transactionVersion: TransactionVersion,
      entryId: DamlLogEntryId,
      coidString: String,
      coidStruct: ValueOuterClass.ContractId,
  ): DamlStateKey =
    ValueCoder.AbsCidDecoder
      .decode(
        sv = transactionVersion,
        stringForm = coidString,
        structForm = coidStruct,
      )
      .fold(
        err => throw Err.DecodeError("ContractId", s"Cannot decode contract id: $err"),
        contractIdToStateKey
      )

}
