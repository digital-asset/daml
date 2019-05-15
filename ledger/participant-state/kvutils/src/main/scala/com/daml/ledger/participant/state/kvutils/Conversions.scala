// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{Configuration, SubmittedTransaction, SubmitterInfo}
import com.digitalasset.daml.lf.data.Ref.{Party, LedgerName}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.{
  Transaction,
  TransactionOuterClass,
  TransactionVersions,
  VersionedTransaction
}
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  NodeId,
  RelativeContractId,
  VContractId
}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.ValueOuterClass
import com.digitalasset.daml.lf.transaction.TransactionCoder
import com.digitalasset.daml.lf.value.ValueCoder
import com.digitalasset.platform.services.time.TimeModel
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

import scala.util.Try

/** Internal utilities for converting between protobuf messages and our scala
  * data structures.
  */
private[kvutils] object Conversions {

  def toAbsCoid(txId: DamlLogEntryId, coid: VContractId): AbsoluteContractId = {
    val hexTxId =
      BaseEncoding.base16.encode(txId.getEntryId.toByteArray)
    coid match {
      case a @ AbsoluteContractId(_) => a
      case RelativeContractId(txnid) =>
        // NOTE(JM): Must be in sync with [[absoluteContractIdToLogEntryId]] and
        // [[absoluteContractIdToStateKey]].
        AbsoluteContractId(LedgerName.assertFromString(s"$hexTxId:${txnid.index}"))
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
      .setContractId(
        DamlContractId.newBuilder
          .setEntryId(entryId)
          .setNodeId(rcoid.txnid.index.toLong)
          .build
      )
      .build

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
      applicationId = subInfo.getApplicationId,
      commandId = subInfo.getCommandId,
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

  // FIXME(JM): Should we have a well-defined schema for this?
  private val cidEncoder: ValueCoder.EncodeCid[VContractId] = {
    val asStruct: VContractId => (String, Boolean) = {
      case RelativeContractId(nid) => (s"~${nid.index}", true)
      case AbsoluteContractId(coid) => (s"$coid", false)
    }
    ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
  }
  private val cidDecoder: ValueCoder.DecodeCid[VContractId] = {
    def fromString(x: String): Either[DecodeError, VContractId] = {
      if (x.startsWith("~"))
        Try(x.tail.toInt).toOption match {
          case None =>
            Left(DecodeError(s"Invalid relative contract id: $x"))
          case Some(i) =>
            Right(RelativeContractId(NodeId.unsafeFromIndex(i)))
        } else
        LedgerName
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
  private val valEncoder: TransactionCoder.EncodeVal[Transaction.Value[VContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(cidEncoder, a).map((a.version, _))
  private val valDecoder: ValueOuterClass.VersionedValue => Either[
    ValueCoder.DecodeError,
    Transaction.Value[VContractId]] =
    a => ValueCoder.decodeVersionedValue(cidDecoder, a)

}
