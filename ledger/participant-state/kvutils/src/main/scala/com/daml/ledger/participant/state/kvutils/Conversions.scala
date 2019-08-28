// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.backport.TimeModel
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{
  Configuration,
  PackageId,
  SubmittedTransaction,
  SubmitterInfo
}
import com.digitalasset.daml.lf.data.Ref.{ContractIdString, LedgerString, Party}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  NodeId,
  RelativeContractId,
  VersionedValue
}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.google.common.io.BaseEncoding
import com.google.protobuf.{ByteString, Empty}

import scala.util.{Failure, Success, Try}

/** Internal utilities for converting between protobuf messages and our scala
  * data structures.
  */
private[kvutils] object Conversions {

  val configurationStateKey: DamlStateKey =
    DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build

  def partyStateKey(party: String): DamlStateKey =
    DamlStateKey.newBuilder.setParty(party).build

  def packageStateKey(packageId: PackageId): DamlStateKey =
    DamlStateKey.newBuilder.setPackageId(packageId).build

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
    AbsoluteContractId(ContractIdString.assertFromString(s"$hexTxId:${coid.getNodeId}"))
  }

  def stateKeyToContractId(key: DamlStateKey): AbsoluteContractId = {
    decodeContractId(key.getContractId)
  }

  def encodeContractKey(key: GlobalKey): DamlContractKey = {
    val encodedValue = valEncoder(key.key)
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
      forceAbsoluteContractIds(
        valDecoder(key.getKey)
          .fold(
            err =>
              throw Err
                .DecodeError("ContractKey", "Cannot decode key: $err"),
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

  def buildDamlConfiguration(config: Configuration): DamlConfiguration = {
    val tm = config.timeModel
    DamlConfiguration.newBuilder
      .setGeneration(config.generation)
      .setAuthorizedParticipantId(config.authorizedParticipantId.fold("")(identity))
      .setOpenWorld(config.openWorld)
      .setTimeModel(
        DamlTimeModel.newBuilder
          .setMaxClockSkew(buildDuration(tm.maxClockSkew))
          .setMinTransactionLatency(buildDuration(tm.minTransactionLatency))
          .setMaxTtl(buildDuration(tm.maxTtl))
      )
      .build
  }

  def parseDamlConfiguration(config: DamlConfiguration): Try[Configuration] =
    for {
      tm <- if (config.hasTimeModel)
        Success(config.getTimeModel)
      else
        Failure(Err.DecodeError("Configuration", "No time model"))
      parsedTM <- TimeModel(
        maxClockSkew = parseDuration(tm.getMaxClockSkew),
        minTransactionLatency = parseDuration(tm.getMinTransactionLatency),
        maxTtl = parseDuration(tm.getMaxTtl)
      )
      authPidString = config.getAuthorizedParticipantId
      authPid <- if (authPidString.isEmpty)
        Success(None)
      else
        LedgerString
          .fromString(config.getAuthorizedParticipantId)
          .fold(
            err => Failure(Err.DecodeError("Configuration", err)),
            ls => Success(Some(ls))
          )

      parsedConfig = Configuration(
        generation = config.getGeneration,
        timeModel = parsedTM,
        authorizedParticipantId = authPid,
        openWorld = config.getOpenWorld
      )
    } yield parsedConfig

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
      .fold(err => throw Err.EncodeError("Transaction", err.errorMessage), identity)
  }

  def decodeTransaction(tx: TransactionOuterClass.Transaction): SubmittedTransaction = {
    TransactionCoder
      .decodeVersionedTransaction(
        nidDecoder,
        cidDecoder,
        tx
      )
      .fold(err => throw Err.DecodeError("Transaction", err.errorMessage), _.transaction)
  }

  def decodeContractInstance(coinst: TransactionOuterClass.ContractInstance)
    : Value.ContractInst[VersionedValue[AbsoluteContractId]] =
    TransactionCoder
      .decodeContractInstance(absValDecoder, coinst)
      .fold(
        err => throw Err.DecodeError("ContractInstance", err.errorMessage),
        coinst => coinst.mapValue(forceAbsoluteContractIds))

  def encodeContractInstance(coinst: Value.ContractInst[VersionedValue[AbsoluteContractId]])
    : TransactionOuterClass.ContractInstance =
    TransactionCoder
      .encodeContractInstance(absValEncoder, coinst)
      .fold(err => throw Err.InternalError(s"encodeContractInstance failed: $err"), identity)

  def forceAbsoluteContractIds(v: VersionedValue[ContractId]): VersionedValue[AbsoluteContractId] =
    v.mapContractId {
      case _: RelativeContractId =>
        throw Err.InternalError("Relative contract identifier encountered in contract key!")
      case acoid: AbsoluteContractId => acoid
    }

  def contractIdStructOrStringToStateKey(
      entryId: DamlLogEntryId,
      coidString: String,
      coidStruct: ValueOuterClass.ContractId): DamlStateKey = {
    val result =
      if (coidString.isEmpty)
        cidDecoder.fromStruct(coidStruct.getContractId, coidStruct.getRelative)
      else
        cidDecoder.fromString(coidString)

    result match {
      case Left(err) =>
        throw Err.DecodeError("ContractId", s"Cannot decode contract id: $err")
      case Right(rcoid: RelativeContractId) =>
        relativeContractIdToStateKey(entryId, rcoid)
      case Right(acoid: AbsoluteContractId) =>
        absoluteContractIdToStateKey(acoid)
    }
  }

  // FIXME(JM): Should we have a well-defined schema for this?
  private val cidEncoder: ValueCoder.EncodeCid[ContractId] = {
    val asStruct: ContractId => (String, Boolean) = {
      case RelativeContractId(nid) => (s"~${nid.index}", true)
      case AbsoluteContractId(coid) => (s"$coid", false)
    }

    ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
  }
  val cidDecoder: ValueCoder.DecodeCid[ContractId] = {
    def fromString(x: String): Either[DecodeError, ContractId] = {
      if (x.startsWith("~")) {
        Try(x.tail.toInt).toOption match {
          case None =>
            Left(DecodeError(s"Invalid relative contract id: $x"))
          case Some(i) =>
            Right(RelativeContractId(NodeId.unsafeFromIndex(i)))
        }
      } else {
        ContractIdString
          .fromString(x)
          .left
          .map(e => DecodeError(s"Invalid absolute contract id: $e"))
          .map(AbsoluteContractId)
      }
    }

    ValueCoder.DecodeCid(
      fromString, {
        case (i, rel) =>
          val coid = fromString(i)
          assert(coid.isLeft || rel == coid.right.get.isInstanceOf[RelativeContractId])
          coid
      }
    )
  }

  private val absCidEncoder: ValueCoder.EncodeCid[AbsoluteContractId] = {
    val asStruct: AbsoluteContractId => (String, Boolean) =
      coid => (coid.coid.toString, false)

    ValueCoder.EncodeCid(asStruct(_)._1, asStruct)
  }

  private val absCidDecoder: ValueCoder.DecodeCid[AbsoluteContractId] = {
    def fromString(x: String): Either[DecodeError, AbsoluteContractId] = {
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

  private val absValEncoder: TransactionCoder.EncodeVal[Transaction.Value[AbsoluteContractId]] =
    a => ValueCoder.encodeVersionedValueWithCustomVersion(absCidEncoder, a).map((a.version, _))

  private val absValDecoder: ValueOuterClass.VersionedValue => Either[
    ValueCoder.DecodeError,
    Transaction.Value[AbsoluteContractId]] =
    a => ValueCoder.decodeVersionedValue(absCidDecoder, a)

}
