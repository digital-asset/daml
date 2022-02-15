// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.io.StringWriter
import java.time.{Duration, Instant}

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection
import com.daml.ledger.participant.state.kvutils.committer.transaction.Rejection.{
  ExternallyInconsistentTransaction,
  InternallyInconsistentTransaction,
}
import com.daml.ledger.participant.state.kvutils.store.events.DamlSubmitterInfo.DeduplicationPeriodCase
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionBlindingInfo.{
  DisclosureEntry,
  DivulgenceEntry,
}
import com.daml.ledger.participant.state.kvutils.store.events.{
  CausalMonotonicityViolated,
  DamlSubmitterInfo,
  DamlTransactionBlindingInfo,
  DamlTransactionRejectionEntry,
  DuplicateKeys,
  InconsistentContracts,
  InconsistentKeys,
  InvalidLedgerTime,
  InvalidParticipantState,
  MissingInputState,
  PartiesNotKnownOnLedger,
  RecordTimeOutOfRange,
  SubmitterCannotActViaParticipant,
  SubmittingPartyNotKnownOnLedger,
  ValidationFailure,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupKey,
  DamlContractKey,
  DamlStateKey,
  DamlSubmissionDedupKey,
  Identifier,
}
import com.daml.ledger.participant.state.kvutils.updates.TransactionRejections._
import com.daml.ledger.participant.state.v2.Update.CommandRejected.FinalReason
import com.daml.ledger.participant.state.v2.{CompletionInfo, SubmitterInfo}
import com.daml.lf.data.Ref.{DottedName, ModuleName, PackageId, QualifiedName}
import com.daml.lf.data.Relation.Relation
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{Ref, Time}
import com.daml.lf.kv.ConversionError
import com.daml.lf.kv.contracts.{ContractConversions, RawContractInstance}
import com.daml.lf.kv.transactions.{RawTransaction, TransactionConversions}
import com.daml.lf.transaction._
import com.daml.lf.value.Value.ContractId
import com.daml.lf.value.Value
import com.daml.lf.{crypto, data}
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.Empty

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Utilities for converting between protobuf messages and our scala
  * data structures.
  */
object Conversions {

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
      .setTemplateId(encodeIdentifier(key.templateId))
      .setHash(key.hash.bytes.toByteString)
      .build
  }

  def encodeContractKey(tmplId: Ref.Identifier, key: Value): DamlContractKey =
    encodeGlobalKey(
      GlobalKey
        .build(tmplId, key)
        .fold(msg => throw Err.InvalidSubmission(msg), identity)
    )

  def encodeIdentifier(id: Ref.Identifier): Identifier =
    Identifier
      .newBuilder()
      .setPackageId(id.packageId)
      .addAllModuleName((id.qualifiedName.module.segments.toSeq: Seq[String]).asJava)
      .addAllName((id.qualifiedName.name.segments.toSeq: Seq[String]).asJava)
      .build()

  def decodeIdentifier(id: Identifier): Either[Err.DecodeError, Ref.Identifier] =
    for {
      pkgId <- PackageId
        .fromString(id.getPackageId)
        .left
        .map(_ => Err.DecodeError("Identifier", s"Invalid package ID: '${id.getPackageId}'"))

      moduleSegments = id.getModuleNameList.asScala
      module <- ModuleName
        .fromSegments(id.getModuleNameList.asScala)
        .left
        .map(_ =>
          Err.DecodeError(
            "Identifier",
            s"Invalid module segments: ${moduleSegments.mkString("'", ", ", "'")}",
          )
        )

      nameSegments = id.getNameList.asScala
      name <- DottedName
        .fromSegments(nameSegments)
        .left
        .map(_ =>
          Err.DecodeError(
            "Identifier",
            s"Invalid name segments: ${nameSegments.mkString("'", ", ", "'")}",
          )
        )
    } yield Ref.Identifier(pkgId, QualifiedName(module, name))

  def globalKeyToStateKey(key: GlobalKey): DamlStateKey =
    DamlStateKey.newBuilder.setContractKey(encodeGlobalKey(key)).build

  def contractKeyToStateKey(templateId: Ref.Identifier, key: Value): DamlStateKey =
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
      .setSubmissionId(subInfo.submissionId.getOrElse(""))
    subInfo.deduplicationPeriod match {
      case DeduplicationPeriod.DeduplicationDuration(duration) =>
        submitterInfoBuilder.setDeduplicationDuration(buildDuration(duration))
      case DeduplicationPeriod.DeduplicationOffset(offset) =>
        submitterInfoBuilder.setDeduplicationOffset(offset.toHexString)
    }
    submitterInfoBuilder.build
  }

  @nowarn("msg=deprecated")
  def parseCompletionInfo(
      recordTime: Timestamp,
      subInfo: DamlSubmitterInfo,
  ): CompletionInfo = {
    val deduplicationPeriod = subInfo.getDeduplicationPeriodCase match {
      case DeduplicationPeriodCase.DEDUPLICATION_DURATION =>
        Some(
          DeduplicationPeriod.DeduplicationDuration(parseDuration(subInfo.getDeduplicationDuration))
        )
      case DeduplicationPeriodCase.DEDUPLICATION_OFFSET =>
        Some(
          DeduplicationPeriod.DeduplicationOffset(
            Offset.fromHexString(Ref.HexString.assertFromString(subInfo.getDeduplicationOffset))
          )
        )
      case DeduplicationPeriodCase.DEDUPLICATE_UNTIL =>
        // For backwards compatibility with rejections generated by participant.state.v1 API.
        // As the deduplicate until timestamp is always relative to record time, we take the duration
        // between record time and the previous timestamp as the deduplication period (duration).
        val until = parseInstant(subInfo.getDeduplicateUntil)
        val duration = Duration.between(recordTime.toInstant, until).abs()
        Some(
          DeduplicationPeriod.DeduplicationDuration(duration)
        )
      case DeduplicationPeriodCase.DEDUPLICATIONPERIOD_NOT_SET =>
        None
    }
    CompletionInfo(
      actAs = subInfo.getSubmittersList.asScala.toList.map(Ref.Party.assertFromString),
      applicationId = Ref.ApplicationId.assertFromString(subInfo.getApplicationId),
      commandId = Ref.LedgerString.assertFromString(subInfo.getCommandId),
      optDeduplicationPeriod = deduplicationPeriod,
      submissionId = Option(subInfo.getSubmissionId)
        .filter(_.nonEmpty)
        .map(
          Ref.SubmissionId.assertFromString
        ),
      statistics = None, // TODO Ledger Metering
    )
  }

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

  def parseInstant(ts: Time.Timestamp): Instant = parseInstant(buildTimestamp(ts))

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

  def assertEncodeTransaction(versionedTransaction: VersionedTransaction): RawTransaction =
    TransactionConversions
      .encodeTransaction(versionedTransaction)
      .fold(err => throw Err.EncodeError("Transaction", err.errorMessage), identity)

  def assertDecodeTransaction(rawTransaction: RawTransaction): VersionedTransaction =
    assertDecode("Transaction", TransactionConversions.decodeTransaction(rawTransaction))

  def assertDecodeContractInstance(
      rawContractInstance: RawContractInstance
  ): Value.VersionedContractInstance =
    assertDecode(
      "ContractInstance",
      ContractConversions.decodeContractInstance(rawContractInstance),
    )

  private def assertDecode[X](context: => String, x: Either[ConversionError, X]): X =
    x.fold(err => throw Err.DecodeError(context, err.errorMessage), identity)

  /** Encodes a [[BlindingInfo]] into protobuf (i.e., [[DamlTransactionBlindingInfo]]).
    * It is consensus-safe because it does so deterministically.
    */
  def encodeBlindingInfo(
      blindingInfo: BlindingInfo,
      divulgedContracts: Map[ContractId, RawContractInstance],
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
        TransactionConversions.decodeTransactionNodeId(
          RawTransaction.NodeId(disclosureEntry.getNodeId)
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
  ): Either[Seq[String], Map[ContractId, RawContractInstance]] = {
    val divulgences = damlTransactionBlindingInfo.getDivulgencesList.asScala.toVector
    if (divulgences.isEmpty) {
      Right(Map.empty)
    } else {
      val resultAccumulator: Either[Seq[String], mutable.Builder[
        (ContractId, RawContractInstance),
        Map[ContractId, RawContractInstance],
      ]] = Right(Map.newBuilder)
      divulgences
        .foldLeft(resultAccumulator) {
          case (Right(contractInstanceIndex), divulgenceEntry) =>
            if (divulgenceEntry.getRawContractInstance.isEmpty) {
              Left(Vector(divulgenceEntry.getContractId))
            } else {
              val contractId = decodeContractId(divulgenceEntry.getContractId)
              val rawContractInstance = RawContractInstance(divulgenceEntry.getRawContractInstance)
              Right(contractInstanceIndex += (contractId -> rawContractInstance))
            }
          case (Left(missingContracts), divulgenceEntry) =>
            // If populated by an older version of the KV WriteService, the contract instances will be missing.
            // Hence, we assume that, if one is missing, all are and return the list of missing ids.
            if (divulgenceEntry.getRawContractInstance.isEmpty) {
              Left(missingContracts :+ divulgenceEntry.getContractId)
            } else {
              Left(missingContracts)
            }
        }
        .map(_.result())
    }
  }

  def encodeTransactionRejectionEntry(
      submitterInfo: DamlSubmitterInfo,
      rejection: Rejection,
  ): DamlTransactionRejectionEntry.Builder = {
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(submitterInfo)
      .setDefiniteAnswer(false)

    rejection match {
      case Rejection.ValidationFailure(error) =>
        builder.setValidationFailure(
          ValidationFailure.newBuilder().setDetails(error.message)
        )
      case InternallyInconsistentTransaction.DuplicateKeys =>
        builder.setInternallyDuplicateKeys(DuplicateKeys.newBuilder())
      case InternallyInconsistentTransaction.InconsistentKeys =>
        builder.setInternallyInconsistentKeys(InconsistentKeys.newBuilder())
      case ExternallyInconsistentTransaction.InconsistentContracts =>
        builder.setExternallyInconsistentContracts(
          InconsistentContracts.newBuilder()
        )
      case ExternallyInconsistentTransaction.DuplicateKeys =>
        builder.setExternallyDuplicateKeys(DuplicateKeys.newBuilder())
      case ExternallyInconsistentTransaction.InconsistentKeys =>
        builder.setExternallyInconsistentKeys(InconsistentKeys.newBuilder())
      case Rejection.MissingInputState(key) =>
        builder.setMissingInputState(
          MissingInputState.newBuilder().setKey(key)
        )
      case Rejection.InvalidParticipantState(error) =>
        builder.setInvalidParticipantState(
          InvalidParticipantState
            .newBuilder()
            .setDetails(error.getMessage)
            .putAllMetadata(error.getMetadata.asJava)
        )
      case Rejection.LedgerTimeOutOfRange(outOfRange) =>
        builder.setInvalidLedgerTime(
          InvalidLedgerTime
            .newBuilder()
            .setDetails(outOfRange.message)
            .setLedgerTime(buildTimestamp(outOfRange.ledgerTime))
            .setLowerBound(buildTimestamp(outOfRange.lowerBound))
            .setUpperBound(buildTimestamp(outOfRange.upperBound))
        )
      case Rejection.RecordTimeOutOfRange(minimumRecordTime, maximumRecordTime) =>
        builder.setRecordTimeOutOfRange(
          RecordTimeOutOfRange
            .newBuilder()
            .setMinimumRecordTime(buildTimestamp(minimumRecordTime))
            .setMaximumRecordTime(buildTimestamp(maximumRecordTime))
        )
      case Rejection.CausalMonotonicityViolated =>
        builder.setCausalMonotonicityViolated(
          CausalMonotonicityViolated.newBuilder()
        )
      case Rejection.SubmittingPartyNotKnownOnLedger(submitter) =>
        builder.setSubmittingPartyNotKnownOnLedger(
          SubmittingPartyNotKnownOnLedger
            .newBuilder()
            .setSubmitterParty(submitter)
        )
      case Rejection.PartiesNotKnownOnLedger(parties) =>
        val stringParties: Iterable[String] = parties
        builder.setPartiesNotKnownOnLedger(
          PartiesNotKnownOnLedger
            .newBuilder()
            .addAllParties(stringParties.asJava)
        )
      case rejection @ Rejection.SubmitterCannotActViaParticipant(submitter, participantId) =>
        builder.setSubmitterCannotActViaParticipant(
          SubmitterCannotActViaParticipant
            .newBuilder()
            .setSubmitterParty(submitter)
            .setParticipantId(participantId)
            .setDetails(rejection.description)
        )
    }
    builder
  }

  @nowarn("msg=deprecated")
  def decodeTransactionRejectionEntry(
      entry: DamlTransactionRejectionEntry
  )(implicit loggingContext: ContextualizedErrorLogger): FinalReason =
    FinalReason(entry.getReasonCase match {
      case DamlTransactionRejectionEntry.ReasonCase.INVALID_LEDGER_TIME =>
        val rejection = entry.getInvalidLedgerTime
        invalidLedgerTimeStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.DISPUTED =>
        val rejection = entry.getDisputed
        disputedStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT =>
        val rejection = entry.getSubmitterCannotActViaParticipant
        submitterCannotActViaParticipantStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.INCONSISTENT =>
        val rejection = entry.getInconsistent
        inconsistentStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.RESOURCES_EXHAUSTED =>
        val rejection = entry.getResourcesExhausted
        resourceExhaustedStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.DUPLICATE_COMMAND =>
        duplicateCommandStatus(entry)
      case DamlTransactionRejectionEntry.ReasonCase.PARTY_NOT_KNOWN_ON_LEDGER =>
        val rejection = entry.getPartyNotKnownOnLedger
        partyNotKnownOnLedgerStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.VALIDATION_FAILURE =>
        val rejection = entry.getValidationFailure
        validationFailureStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.INTERNALLY_DUPLICATE_KEYS =>
        internallyDuplicateKeysStatus()
      case DamlTransactionRejectionEntry.ReasonCase.INTERNALLY_INCONSISTENT_KEYS =>
        internallyInconsistentKeysStatus()
      case DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_CONTRACTS =>
        externallyInconsistentContractsStatus()
      case DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_DUPLICATE_KEYS =>
        externallyDuplicateKeysStatus()
      case DamlTransactionRejectionEntry.ReasonCase.EXTERNALLY_INCONSISTENT_KEYS =>
        externallyInconsistentKeysStatus()
      case DamlTransactionRejectionEntry.ReasonCase.MISSING_INPUT_STATE =>
        val rejection = entry.getMissingInputState
        missingInputStateStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.RECORD_TIME_OUT_OF_RANGE =>
        val rejection = entry.getRecordTimeOutOfRange
        recordTimeOutOfRangeStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.CAUSAL_MONOTONICITY_VIOLATED =>
        causalMonotonicityViolatedStatus()
      case DamlTransactionRejectionEntry.ReasonCase.SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER =>
        val rejection = entry.getSubmittingPartyNotKnownOnLedger
        submittingPartyNotKnownOnLedgerStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.PARTIES_NOT_KNOWN_ON_LEDGER =>
        val rejection = entry.getPartiesNotKnownOnLedger
        partiesNotKnownOnLedgerStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.INVALID_PARTICIPANT_STATE =>
        val rejection = entry.getInvalidParticipantState
        invalidParticipantStateStatus(rejection)
      case DamlTransactionRejectionEntry.ReasonCase.REASON_NOT_SET =>
        rejectionReasonNotSetStatus()
    })

  def objectToJsonString(obj: Object): String = {
    val stringWriter = new StringWriter
    val objectMapper = new ObjectMapper
    objectMapper.writeValue(stringWriter, obj)
    stringWriter.toString
  }

  private def encodeParties(parties: Set[Ref.Party]): List[String] =
    (parties.toList: List[String]).sorted

  private def encodeDisclosureEntry(disclosureEntry: (NodeId, Set[Ref.Party])): DisclosureEntry =
    DisclosureEntry.newBuilder
      .setNodeId(TransactionConversions.encodeTransactionNodeId(disclosureEntry._1).value)
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
      rawContractInstance: RawContractInstance,
  ): DivulgenceEntry =
    DivulgenceEntry.newBuilder
      .setContractId(contractIdToString(contractId))
      .addAllDivulgedToLocalParties(encodeParties(divulgedTo).asJava)
      .setRawContractInstance(rawContractInstance.byteString)
      .build

  private def encodeDivulgence(
      divulgence: Relation[ContractId, Ref.Party],
      divulgedContractsIndex: Map[ContractId, RawContractInstance],
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
