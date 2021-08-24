// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.SubmitterInfo
import com.daml.lf.data.Relation.Relation
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.transaction._
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{Value, ValueCoder, ValueOuterClass}
import com.daml.lf.{crypto, data}
import com.google.protobuf.Empty

import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Utilities for converting between protobuf messages and our scala
  * data structures.
  */
private[state] object Conversions {

  val configurationStateKey: DamlStateKey =
    DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build

  def partyStateKey(party: String): DamlStateKey =
    DamlStateKey.newBuilder.setParty(party).build

  def packageStateKey(packageId: Ref.PackageId): DamlStateKey =
    DamlStateKey.newBuilder.setPackageId(packageId).build

  def contractIdToString(contractId: ContractId): String = contractId.coid

  def contractIdToStateKey(acoid: ContractId): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId(contractIdToString(acoid))
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

  def encodeContractKey(tmplId: Ref.Identifier, key: Value[ContractId]): DamlContractKey =
    encodeGlobalKey(
      GlobalKey
        .build(tmplId, key)
        .fold(msg => throw Err.InvalidSubmission(msg), identity)
    )

  def decodeIdentifier(protoIdent: ValueOuterClass.Identifier): Ref.Identifier =
    assertDecode("Identifier", ValueCoder.decodeIdentifier(protoIdent))

  def globalKeyToStateKey(key: GlobalKey): DamlStateKey =
    DamlStateKey.newBuilder.setContractKey(encodeGlobalKey(key)).build

  def contractKeyToStateKey(templateId: Ref.Identifier, key: Value[ContractId]): DamlStateKey =
    DamlStateKey.newBuilder.setContractKey(encodeContractKey(templateId, key)).build

  def commandDedupKey(subInfo: DamlSubmitterInfo): DamlStateKey = {
    val sortedUniqueSubmitters =
      if (subInfo.getSubmittersCount == 1)
        subInfo.getSubmittersList
      else
        subInfo.getSubmittersList.asScala.distinct.sorted.asJava
    DamlStateKey.newBuilder
      .setCommandDedup(
        DamlCommandDedupKey.newBuilder
          .addAllSubmitters(sortedUniqueSubmitters)
          .setApplicationId(subInfo.getApplicationId)
          .setCommandId(subInfo.getCommandId)
          .build
      )
      .build
  }

  def submissionDedupKey(
      participantId: String,
      submissionId: String,
      submissionKind: DamlSubmissionDedupKey.SubmissionKind,
  ): DamlStateKey = {
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
      DamlSubmissionDedupKey.SubmissionKind.PACKAGE_UPLOAD,
    )

  def partyAllocationDedupKey(participantId: String, submissionId: String): DamlStateKey =
    submissionDedupKey(
      participantId,
      submissionId,
      DamlSubmissionDedupKey.SubmissionKind.PARTY_ALLOCATION,
    )

  def configDedupKey(participantId: String, submissionId: String): DamlStateKey =
    submissionDedupKey(
      participantId,
      submissionId,
      DamlSubmissionDedupKey.SubmissionKind.CONFIGURATION,
    )

  def buildSubmitterInfo(subInfo: SubmitterInfo): DamlSubmitterInfo = {
    val submitterInfoBuilder = DamlSubmitterInfo.newBuilder
      .addAllSubmitters((subInfo.actAs: List[String]).asJava)
      .setApplicationId(subInfo.applicationId)
      .setCommandId(subInfo.commandId)
      .setSubmissionId(subInfo.submissionId)
    subInfo.deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) =>
        submitterInfoBuilder.setDeduplicationTime(buildDuration(duration))
      case DeduplicationPeriod.DeduplicationOffset(offset) =>
        submitterInfoBuilder.setDeduplicationOffset(offset.toHexString)
    }
    submitterInfoBuilder.build
  }

  def parseCompletionInfo(subInfo: DamlSubmitterInfo): CompletionInfo = {
    val deduplicationPeriod = subInfo.getDeduplicationCase match {
      case DeduplicationCase.DEDUPLICATION_TIME =>
        Some(DeduplicationPeriod.DeduplicationDuration(parseDuration(subInfo.getDeduplicationTime)))
      case DeduplicationCase.DEDUPLICATION_OFFSET =>
        Some(
          DeduplicationPeriod.DeduplicationOffset(
            Offset.fromHexString(Ref.HexString.assertFromString(subInfo.getDeduplicationOffset))
          )
        )
      case DeduplicationCase.DEDUPLICATE_UNTIL => //backwards compat
        None //FIXME can we convert from a future timestamp into a sensible dedup duration?
      case DeduplicationCase.DEDUPLICATION_NOT_SET =>
        None
    }
    CompletionInfo(
      actAs = subInfo.getSubmittersList.asScala.toList.map(Ref.Party.assertFromString),
      applicationId = Ref.LedgerString.assertFromString(subInfo.getApplicationId),
      commandId = Ref.LedgerString.assertFromString(subInfo.getCommandId),
      optDeduplicationPeriod = deduplicationPeriod,
      submissionId = Ref.SubmissionId.assertFromString(
        subInfo.getSubmissionId
      ), // FIXME this can be missing, what to do what to do
    )

  def buildTimestamp(ts: Time.Timestamp): com.google.protobuf.Timestamp =
    buildTimestamp(ts.toInstant)

  def buildTimestamp(instant: Instant): com.google.protobuf.Timestamp =
    com.google.protobuf.Timestamp.newBuilder
      .setSeconds(instant.getEpochSecond)
      .setNanos(instant.getNano)
      .build

  def parseTimestamp(ts: com.google.protobuf.Timestamp): Time.Timestamp =
    Time.Timestamp.assertFromInstant(parseInstant(ts))

  def parseInstant(ts: com.google.protobuf.Timestamp): Instant =
    Instant.ofEpochSecond(ts.getSeconds, ts.getNanos.toLong)

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
          tx,
        ),
    )

  def decodeVersionedValue(protoValue: ValueOuterClass.VersionedValue): VersionedValue[ContractId] =
    assertDecode(
      "ContractInstance",
      ValueCoder.decodeVersionedValue(ValueCoder.CidDecoder, protoValue),
    )

  def decodeContractInstance(
      coinst: TransactionOuterClass.ContractInstance
  ): Value.ContractInst[VersionedValue[ContractId]] =
    assertDecode(
      "ContractInstance",
      TransactionCoder
        .decodeVersionedContractInstance(ValueCoder.CidDecoder, coinst),
    )

  def encodeContractInstance(
      coinst: Value.ContractInst[VersionedValue[Value.ContractId]]
  ): TransactionOuterClass.ContractInstance =
    assertEncode(
      "ContractInstance",
      TransactionCoder.encodeContractInstance(ValueCoder.CidEncoder, coinst),
    )

  def contractIdStructOrStringToStateKey[A](
      coidStruct: ValueOuterClass.ContractId
  ): DamlStateKey =
    contractIdToStateKey(
      assertDecode(
        "ContractId",
        ValueCoder.CidDecoder.decode(
          structForm = coidStruct
        ),
      )
    )

  def encodeTransactionNodeId(nodeId: NodeId): String =
    nodeId.index.toString

  def decodeTransactionNodeId(transactionNodeId: String): NodeId =
    NodeId(transactionNodeId.toInt)

  /** Encodes a [[BlindingInfo]] into protobuf (i.e., [[DamlTransactionBlindingInfo]]).
    * It is consensus-safe because it does so deterministically.
    */
  def encodeBlindingInfo(
      blindingInfo: BlindingInfo,
      divulgedContracts: Map[ContractId, TransactionOuterClass.ContractInstance],
  ): DamlTransactionBlindingInfo =
    DamlTransactionBlindingInfo.newBuilder
      .addAllDisclosures(encodeDisclosure(blindingInfo.disclosure).asJava)
      .addAllDivulgences(encodeDivulgence(blindingInfo.divulgence, divulgedContracts).asJava)
      .build

  def decodeBlindingInfo(
      damlTransactionBlindingInfo: DamlTransactionBlindingInfo
  ): BlindingInfo = {
    val blindingInfoDivulgence =
      damlTransactionBlindingInfo.getDivulgencesList.asScala.iterator.map { divulgenceEntry =>
        val contractId = decodeContractId(divulgenceEntry.getContractId)
        val divulgedTo = divulgenceEntry.getDivulgedToLocalPartiesList.asScala.toSet
          .map(Ref.Party.assertFromString)
        contractId -> divulgedTo
      }.toMap

    val blindingInfoDisclosure = damlTransactionBlindingInfo.getDisclosuresList.asScala.map {
      disclosureEntry =>
        decodeTransactionNodeId(
          disclosureEntry.getNodeId
        ) -> disclosureEntry.getDisclosedToLocalPartiesList.asScala.toSet
          .map(Ref.Party.assertFromString)
    }.toMap

    BlindingInfo(
      disclosure = blindingInfoDisclosure,
      divulgence = blindingInfoDivulgence,
    )
  }

  def extractDivulgedContracts(
      damlTransactionBlindingInfo: DamlTransactionBlindingInfo
  ): Either[Seq[String], Map[ContractId, Value.ContractInst[VersionedValue[ContractId]]]] = {
    val divulgences = damlTransactionBlindingInfo.getDivulgencesList.asScala.toVector
    if (divulgences.isEmpty) {
      Right(Map.empty)
    } else {
      val resultAccumulator: Either[Seq[String], mutable.Builder[
        (ContractId, Value.ContractInst[VersionedValue[ContractId]]),
        Map[ContractId, Value.ContractInst[VersionedValue[ContractId]]],
      ]] = Right(Map.newBuilder)
      divulgences
        .foldLeft(resultAccumulator) {
          case (Right(contractInstanceIndex), divulgenceEntry) =>
            if (divulgenceEntry.hasContractInstance) {
              val contractId = decodeContractId(divulgenceEntry.getContractId)
              val contractInstance = decodeContractInstance(divulgenceEntry.getContractInstance)
              Right(contractInstanceIndex += (contractId -> contractInstance))
            } else {
              Left(Vector(divulgenceEntry.getContractId))
            }
          case (Left(missingContracts), divulgenceEntry) =>
            // If populated by an older version of the KV WriteService, the contract instances will be missing.
            // Hence, we assume that, if one is missing, all are and return the list of missing ids.
            if (divulgenceEntry.hasContractInstance) {
              Left(missingContracts)
            } else {
              Left(missingContracts :+ divulgenceEntry.getContractId)
            }
        }
        .map(_.result())
    }
  }

  private def encodeParties(parties: Set[Ref.Party]): List[String] =
    (parties.toList: List[String]).sorted

  private def encodeDisclosureEntry(disclosureEntry: (NodeId, Set[Ref.Party])): DisclosureEntry =
    DisclosureEntry.newBuilder
      .setNodeId(encodeTransactionNodeId(disclosureEntry._1))
      .addAllDisclosedToLocalParties(encodeParties(disclosureEntry._2).asJava)
      .build

  private def encodeDisclosure(
      disclosure: Relation[NodeId, Ref.Party]
  ): List[DisclosureEntry] =
    disclosure.toList
      .sortBy(_._1.index)
      .map(encodeDisclosureEntry)

  private def encodeDivulgenceEntry(
      contractId: ContractId,
      divulgedTo: Set[Ref.Party],
      contractInstance: TransactionOuterClass.ContractInstance,
  ): DivulgenceEntry =
    DivulgenceEntry.newBuilder
      .setContractId(contractIdToString(contractId))
      .addAllDivulgedToLocalParties(encodeParties(divulgedTo).asJava)
      .setContractInstance(contractInstance)
      .build

  private def encodeDivulgence(
      divulgence: Relation[ContractId, Ref.Party],
      divulgedContractsIndex: Map[ContractId, TransactionOuterClass.ContractInstance],
  ): List[DivulgenceEntry] =
    divulgence.toList
      .sortBy(_._1.coid)
      .map { case (contractId, party) =>
        val contractInst =
          divulgedContractsIndex.getOrElse(
            contractId,
            throw Err.MissingDivulgedContractInstance(contractId.coid),
          )
        encodeDivulgenceEntry(contractId, party, contractInst)
      }
}
