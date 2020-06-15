// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{PackageId, SubmitterInfo}
import com.daml.lf.crypto
import com.daml.lf.data
import com.daml.lf.data.Ref.{Identifier, LedgerString, Party}
import com.daml.lf.data.Time
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.transaction._
import com.daml.lf.transaction.VersionTimeline.Implicits._
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.google.protobuf.Empty

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

  def contractIdToStateKey(acoid: ContractId): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId(acoid.coid)
      .build

  def decodeContractId(coid: String): ContractId =
    ContractId.assertFromString(coid)

  def stateKeyToContractId(key: DamlStateKey): ContractId =
    decodeContractId(key.getContractId)

  def encodeGlobalKey(key: GlobalKey): DamlContractKey = {
    DamlContractKey.newBuilder
      .setTemplateId(ValueCoder.encodeIdentifier(key.templateId))
      .setHash(key.hash.bytes.toByteString)
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
      .setDeduplicateUntil(
        buildTimestamp(Time.Timestamp.assertFromInstant(subInfo.deduplicateUntil)))
      .build

  def parseSubmitterInfo(subInfo: DamlSubmitterInfo): SubmitterInfo =
    SubmitterInfo(
      submitter = Party.assertFromString(subInfo.getSubmitter),
      applicationId = LedgerString.assertFromString(subInfo.getApplicationId),
      commandId = LedgerString.assertFromString(subInfo.getCommandId),
      deduplicateUntil = parseTimestamp(subInfo.getDeduplicateUntil).toInstant,
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

  def parseHash(bytes: com.google.protobuf.ByteString): crypto.Hash =
    crypto.Hash.assertFromBytes(data.Bytes.fromByteString(bytes))

  def buildDuration(dur: Duration): com.google.protobuf.Duration = {
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
  }

  def parseDuration(dur: com.google.protobuf.Duration): Duration = {
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)
  }

  private def assertDecode[X](context: => String, x: Either[ValueCoder.DecodeError, X]): X =
    x.fold(err => throw Err.DecodeError(context, err.errorMessage), identity)

  private def assertEncode[X](context: => String, x: Either[ValueCoder.EncodeError, X]): X =
    x.fold(err => throw Err.EncodeError(context, err.errorMessage), identity)

  def encodeTransaction(tx: Transaction.Transaction): TransactionOuterClass.Transaction =
    assertEncode(
      "Transaction",
      TransactionCoder.encodeTransaction(TransactionCoder.NidEncoder, ValueCoder.CidEncoder, tx),
    )

  def decodeTransaction(tx: TransactionOuterClass.Transaction): Transaction.Transaction =
    assertDecode(
      "Transaction",
      TransactionCoder
        .decodeTransaction(
          TransactionCoder.NidDecoder,
          ValueCoder.CidDecoder,
          tx
        ),
    )

  def decodeVersionedValue(protoValue: ValueOuterClass.VersionedValue): VersionedValue[ContractId] =
    assertDecode(
      "ContractInstance",
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, protoValue),
    )

  def decodeContractInstance(coinst: TransactionOuterClass.ContractInstance)
    : Value.ContractInst[VersionedValue[ContractId]] =
    assertDecode(
      "ContractInstance",
      TransactionCoder
        .decodeContractInstance(ValueCoder.CidDecoder, coinst)
    )

  def encodeContractInstance(coinst: Value.ContractInst[VersionedValue[Value.ContractId]])
    : TransactionOuterClass.ContractInstance =
    assertEncode(
      "ContractInstance",
      TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst)
    )

  def forceNoContractIds(v: Value[Value.ContractId]): Value[Nothing] =
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
    contractIdToStateKey(
      assertDecode(
        "ContractId",
        ValueCoder.CidDecoder.decode(
          sv = transactionVersion,
          stringForm = coidString,
          structForm = coidStruct,
        ),
      )
    )

}
