// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{Configuration, SubmittedTransaction, SubmitterInfo}
import com.digitalasset.daml.lf.data.Ref.{ContractIdString, LedgerString, Party}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.{
  Transaction,
  TransactionOuterClass,
  TransactionVersions,
  VersionedTransaction
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  NodeId,
  RelativeContractId,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.ValueOuterClass
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.digitalasset.daml.lf.value.ValueCoder
import com.daml.ledger.participant.state.backport.TimeModel
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.util.Try

/** Internal utilities for converting between protobuf messages and our scala
  * data structures.
  */
private[kvutils] object Conversions {

  def toAbsCoid(txId: DamlLogEntryId, coid: ContractId): AbsoluteContractId = {
    val hexTxId =
      BaseEncoding.base16.encode(txId.getEntryId.toByteArray)
    coid match {
      case a @ AbsoluteContractId(_) => a
      case RelativeContractId(txnid) =>
        // NOTE(JM): Must be in sync with [[absoluteContractIdToLogEntryId]] and
        // [[absoluteContractIdToStateKey]].
        AbsoluteContractId(ContractIdString.assertFromString(s"$hexTxId:${txnid.index}"))
    }
  }

  def absoluteContractIdToLogEntryId(acoid: AbsoluteContractId): (DamlLogEntryId, Int) =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        DamlLogEntryId.newBuilder
          .setEntryId(ByteString.copyFrom(BaseEncoding.base16.decode(hexTxId)))
          .build -> nodeId.toInt
      case _ => sys.error(s"decodeAbsoluteContractId: Cannot decode '$acoid'")
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
      case _ => sys.error(s"decodeAbsoluteContractId: Cannot decode '$acoid'")
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
    AbsoluteContractId(ContractIdString.assertFromString(s"$hexTxId:${coid.getNodeId}"))
  }

  def stateKeyToContractId(key: DamlStateKey): AbsoluteContractId = {
    // FIXME(JM): Graceful error handling
    decodeContractId(key.getContractId)
  }

  def encodeContractKey(key: GlobalKey): DamlContractKey = {
    val encodedValue = valEncoder(key.key)
      .getOrElse(sys.error(s"contractKeyToStateKey: Cannot encode ${key.key}!"))
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
          sys.error(s"decodeContractKey: Cannot decode template id!")
        ),
      forceAbsoluteContractIds(
        valDecoder(key.getKey).getOrElse(sys.error("decodeContractKey: Cannot decode key!")))
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

  def parseDamlConfigurationEntry(config: DamlConfigurationEntry): Configuration = {
    val tm = config.getTimeModel
    Configuration(
      TimeModel(
        maxClockSkew = parseDuration(tm.getMaxClockSkew),
        minTransactionLatency = parseDuration(tm.getMinTransactionLatency),
        maxTtl = parseDuration(tm.getMaxTtl)
      ).get // FIXME(JM): handle error
    )
  }

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
      .encodeTransactionWithCustomVersion(
        nidEncoder,
        cidEncoder,
        VersionedTransaction(TransactionVersions.assignVersion(tx), tx))
      .fold(err => sys.error(s"encodeTransaction error: $err"), identity)
  }

  def decodeTransaction(tx: TransactionOuterClass.Transaction): SubmittedTransaction = {
    TransactionCoder
      .decodeVersionedTransaction(
        nidDecoder,
        cidDecoder,
        tx
      )
      .fold(err => sys.error(s"decodeTransaction error: $err"), _.transaction)
  }

  def forceAbsoluteContractIds(v: VersionedValue[ContractId]): VersionedValue[AbsoluteContractId] =
    v.mapContractId {
      case _: RelativeContractId =>
        sys.error("Relative contract identifier encountered in contract key!")
      case acoid: AbsoluteContractId => acoid
    }

  // FIXME(JM): Should we have a well-defined schema for this?
  private val cidEncoder: ValueCoder.EncodeCid[ContractId] = {
    val asStruct: ContractId => (String, Boolean) = {
      case RelativeContractId(nid) => (s"~${nid.index}", true)
      case AbsoluteContractId(coid) => (s"$coid", false)
    }
    ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
  }
  private val cidDecoder: ValueCoder.DecodeCid[ContractId] = {
    def fromString(x: String): Either[DecodeError, ContractId] = {
      if (x.startsWith("~"))
        Try(x.tail.toInt).toOption match {
          case None =>
            Left(DecodeError(s"Invalid relative contract id: $x"))
          case Some(i) =>
            Right(RelativeContractId(NodeId.unsafeFromIndex(i)))
        } else
        ContractIdString
          .fromString(x)
          .left
          .map(e => DecodeError(s"Invalid absolute contract id: $e"))
          .map(AbsoluteContractId)
    }

    ValueCoder.DecodeCid(
      fromString,
      { case (i, _) => fromString(i) }
    )
  }

  private val nidDecoder: String => Either[ValueCoder.DecodeError, NodeId] =
    nid => Right(NodeId.unsafeFromIndex(nid.toInt))
  private val nidEncoder: TransactionCoder.EncodeNid[NodeId] =
    nid => nid.index.toString
  private val valEncoder: TransactionCoder.EncodeVal[Transaction.Value[ContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(cidEncoder, a).map((a.version, _))
  private val valDecoder: ValueOuterClass.VersionedValue => Either[
    ValueCoder.DecodeError,
    Transaction.Value[ContractId]] =
    a => ValueCoder.decodeVersionedValue(cidDecoder, a)

}
