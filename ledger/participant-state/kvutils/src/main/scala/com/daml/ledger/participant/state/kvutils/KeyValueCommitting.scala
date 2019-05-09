// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{Configuration, PackageId, RejectionReason}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.{Blinding, Engine}
import com.digitalasset.daml.lf.lfpackage.Decode
import com.digitalasset.daml.lf.transaction.Node.NodeCreate
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  NodeId,
  VersionedValue
}
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object KeyValueCommitting {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /** Errors that can result from improper calls to processSubmission.
    * Validation and consistency errors are turned into command rejections.
    * Note that processSubmission can also fail with a protobuf exception,
    * e.g. https://developers.google.com/protocol-buffers/docs/reference/java/com/google/protobuf/InvalidProtocolBufferException.
    */
  sealed trait Err extends RuntimeException with Product with Serializable
  object Err {
    final case class InvalidPayload(message: String) extends Err
    final case class MissingInputLogEntry(entryId: DamlLogEntryId) extends Err
    final case class MissingInputState(key: DamlStateKey) extends Err
    final case class NodeMissingFromLogEntry(entryId: DamlLogEntryId, nodeId: Int) extends Err
    final case class NodeNotACreate(entryId: DamlLogEntryId, nodeId: Int) extends Err
    final case class ArchiveDecodingFailed(packageId: PackageId, reason: String) extends Err
  }

  def packDamlStateKey(key: DamlStateKey): ByteString = key.toByteString
  def unpackDamlStateKey(bytes: ByteString): DamlStateKey = DamlStateKey.parseFrom(bytes)

  def packDamlStateValue(value: DamlStateValue): ByteString = value.toByteString
  def unpackDamlStateValue(bytes: ByteString): DamlStateValue = DamlStateValue.parseFrom(bytes)

  def packDamlLogEntry(entry: DamlLogEntry): ByteString = entry.toByteString
  def unpackDamlLogEntry(bytes: ByteString): DamlLogEntry = DamlLogEntry.parseFrom(bytes)

  def packDamlLogEntryId(entry: DamlLogEntryId): ByteString = entry.toByteString
  def unpackDamlLogEntryId(bytes: ByteString): DamlLogEntryId = DamlLogEntryId.parseFrom(bytes)

  /** Pretty-printing of the entry identifier. Uses the same hexadecimal encoding as is used
    * for absolute contract identifiers.
    */
  def prettyEntryId(entryId: DamlLogEntryId): String =
    BaseEncoding.base16.encode(entryId.getEntryId.toByteArray)

  /** Processes a DAML submission, given the allocated log entry id, the submission and its resolved inputs.
    * Produces the log entry to be committed, and DAML state updates.
    *
    * The caller is expected to resolve the inputs declared in [[DamlSubmission]] prior
    * to calling this method, e.g. by reading [[DamlSubmission!.getInputEntriesList]] and
    * [[DamlSubmission!.getInputStateList]]
    *
    * The caller is expected to store the produced [[DamlLogEntry]] in key-value store at a location
    * that can be accessed through `entryId`. The DAML state updates may create new entries or update
    * existing entries in the key-value store. The concrete key for DAML state entry is obtained by applying
    * [[packDamlStateKey]] to [[DamlStateKey]].
    *
    * @param engine: DAML Engine. This instance should be persistent as it caches package compilation.
    * @param config: Ledger configuration.
    * @param entryId: Log entry id to which this submission is committed.
    * @param recordTime: Record time at which this log entry is committed.
    * @param submission: Submission to commit to the ledger.
    * @param inputLogEntries: Resolved input log entries specified in submission.
    * @param inputState:
    *   Resolved input state specified in submission. Optional to mork that input state was resolved
    *   but not present. Specifically we require the command de-duplication input to be resolved, but don't
    *   expect to be there.
    * @return Log entry to be committed and the DAML state updates to be applied.
    */
  def processSubmission(
      engine: Engine,
      config: Configuration,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      submission: DamlSubmission,
      inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
      inputState: Map[DamlStateKey, Option[DamlStateValue]]
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {

    // Look at what kind of submission this is...
    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.ARCHIVE =>
        val archive = submission.getArchive
        val key = DamlStateKey.newBuilder.setPackageId(archive.getHash).build
        // TODO(JM): We're duplicating the archive data. This way we don't have an indirection
        // to fetch by packageId or when building [[Update]], and we don't have to declare
        // the log entry containing the archive as an input (which would be messy).
        logger.trace(
          s"processSubmission[entryId=${prettyEntryId(entryId)}]: Package ${archive.getHash} committed.")
        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setArchive(archive)
            .build,
          Map(
            key -> DamlStateValue.newBuilder.setArchive(archive).build
          )
        )
      case DamlSubmission.PayloadCase.CONFIGURATION_ENTRY =>
        logger.trace(
          s"processSubmission[entryId=${prettyEntryId(entryId)}]: New configuration committed.")
        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setConfigurationEntry(submission.getConfigurationEntry)
            .build,
          Map.empty
        )
      case DamlSubmission.PayloadCase.TRANSACTION_ENTRY =>
        processTransactionSubmission(
          engine,
          config,
          entryId,
          recordTime,
          submission.getTransactionEntry,
          inputLogEntries,
          inputState
        )

      case DamlSubmission.PayloadCase.PAYLOAD_NOT_SET =>
        throw Err.InvalidPayload("DamlSubmission.payload not set.")
    }
  }

  private def processTransactionSubmission(
      engine: Engine,
      config: Configuration,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      txEntry: DamlTransactionEntry,
      inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
      inputState: Map[DamlStateKey, Option[DamlStateValue]]
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
    val commandId = txEntry.getSubmitterInfo.getCommandId
    def tracelog(msg: String) =
      logger.trace(
        s"processTransactionSubmission[entryId=${prettyEntryId(entryId)}, cmdId=$commandId]: $msg")

    val txLet = parseTimestamp(txEntry.getLedgerEffectiveTime)
    val submitterInfo = txEntry.getSubmitterInfo

    // 1. Verify that this is not a duplicate command submission.
    val dedupKey = commandDedupKey(submitterInfo)
    val dedupEntry = inputState(dedupKey)
    val dedupCheckResult = dedupEntry.isEmpty

    // 2. Verify that the ledger effective time falls within time bounds
    val letCheckResult = config.timeModel.checkLet(
      currentTime = recordTime.toInstant,
      givenLedgerEffectiveTime = txLet.toInstant,
      givenMaximumRecordTime =
        parseTimestamp(txEntry.getSubmitterInfo.getMaximumRecordTime).toInstant
    )

    // Helper to lookup contract instances. We verify the activeness of
    // contract instances here. Since we look up every contract that was
    // an input to a transaction, we do not need to verify the inputs separately.
    def lookupContract(coid: AbsoluteContractId) = {
      val (eid, nid) = absoluteContractIdToLogEntryId(coid)
      val stateKey = absoluteContractIdToStateKey(coid)
      for {
        // Fetch the state of the contract so that activeness and visibility can be checked.
        contractState <- inputState.get(stateKey).flatMap(_.map(_.getContractState)).orElse {
          tracelog(s"lookupContract($coid): Contract state not found!")
          throw Err.MissingInputState(stateKey)
        }
        locallyDisclosedTo = contractState.getLocallyDisclosedToList.asScala.toSet
        divulgedTo = contractState.getDivulgedToList.asScala.toSet
        submitter = submitterInfo.getSubmitter

        // 1. Verify that the submitter can view the contract.
        _ <- if (locallyDisclosedTo.contains(submitter) || divulgedTo.contains(submitter))
          Some(())
        else {
          tracelog(s"lookupContract($coid): Contract not visible to submitter $submitter")
          None
        }

        // 2. Verify that the contract is active
        _ <- if (contractState.hasActiveAt && txLet >= parseTimestamp(contractState.getActiveAt))
          Some(())
        else {
          val activeAtStr =
            if (contractState.hasActiveAt)
              parseTimestamp(contractState.getActiveAt).toString
            else "<activeAt missing>"
          tracelog(
            s"lookupContract($coid): Contract not active (let=$txLet, activeAt=$activeAtStr).")
          None
        }

        // Finally lookup the log entry containing the create node and the contract instance.
        entry = inputLogEntries
          .getOrElse(eid, throw Err.MissingInputLogEntry(eid))
        coinst <- lookupContractInstanceFromLogEntry(eid, entry, nid)
      } yield (coinst)
    }

    // Helper to lookup package from the state. The package contents
    // are stored in the [[DamlLogEntry]], which we find by looking up
    // the DAML state entry at `DamlStateKey(packageId = pkgId)`.
    def lookupPackage(pkgId: PackageId) = {
      val stateKey = DamlStateKey.newBuilder.setPackageId(pkgId).build
      for {
        value <- inputState
          .get(stateKey)
          .flatten
          .orElse {
            throw Err.MissingInputState(stateKey)
          }
        pkg <- value.getValueCase match {
          case DamlStateValue.ValueCase.ARCHIVE =>
            // NOTE(JM): Engine only looks up packages once, compiles and caches,
            // provided that the engine instance is persisted.
            Some(Decode.decodeArchive(value.getArchive)._2)
          case _ =>
            tracelog(s"lookupPackage($pkgId): value not a DAML-LF archive!")
            None
        }
      } yield pkg
    }

    // 3. Verify that the submission conforms to the DAML model
    val relTx = Conversions.decodeTransaction(txEntry.getTransaction)
    val modelCheckResult = engine
      .validate(relTx, txLet)
      .consume(lookupContract, lookupPackage, _ => sys.error("contract keys unimplemented"))

    // Compute blinding info to update contract visibility.
    val blindingInfo = Blinding.blind(relTx)

    (dedupCheckResult, letCheckResult, modelCheckResult) match {
      case (false, _, _) =>
        tracelog("rejected, duplicate command.")
        buildRejectionLogEntry(recordTime, txEntry, RejectionReason.DuplicateCommand)

      case (_, false, _) =>
        tracelog("rejected, maximum record time exceeded.")
        buildRejectionLogEntry(recordTime, txEntry, RejectionReason.MaximumRecordTimeExceeded)

      case (true, true, Left(err)) =>
        tracelog("rejected, transaction disputed.")
        buildRejectionLogEntry(recordTime, txEntry, RejectionReason.Disputed(err.msg)) // FIXME(JM): or detailMsg?

      case (true, true, Right(())) =>
        // All checks passed. Return transaction log entry, and update the DAML state
        // with the committed command and the created and consumed contracts.

        val stateUpdates = scala.collection.mutable.Map.empty[DamlStateKey, DamlStateValue]

        // Add command dedup state entry for command deduplication (checked by step 1 above).
        stateUpdates += commandDedupKey(txEntry.getSubmitterInfo) -> commandDedupValue

        val effects = InputsAndEffects.computeEffects(entryId, relTx)

        // Update contract state entries to mark contracts as consumed (checked by step 3 above)
        effects.consumedContracts.foreach { key =>
          val cs =
            inputState(key).getOrElse(throw Err.MissingInputState(key)).getContractState.toBuilder
          cs.setArchivedAt(buildTimestamp(txLet))
          cs.setArchivedByEntry(entryId)
          stateUpdates += key -> DamlStateValue.newBuilder.setContractState(cs.build).build
        }

        // Add contract state entries to mark contract activeness (checked by step 3 above)
        effects.createdContracts.foreach { key =>
          val cs = DamlContractState.newBuilder
          cs.setActiveAt(buildTimestamp(txLet))
          val localDisclosure =
            blindingInfo.localDisclosure(NodeId.unsafeFromIndex(key.getContractId.getNodeId.toInt))
          cs.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
          stateUpdates += key -> DamlStateValue.newBuilder.setContractState(cs).build
        }

        // Update contract state of divulged contracts
        blindingInfo.globalImplicitDisclosure.foreach {
          case (absCoid, parties) =>
            val key = absoluteContractIdToStateKey(absCoid)
            val cs =
              inputState(key).getOrElse(throw Err.MissingInputState(key)).getContractState.toBuilder
            cs.addAllDivulgedTo((parties: Iterable[String]).asJava)
            stateUpdates += key -> DamlStateValue.newBuilder.setContractState(cs.build).build
        }

        tracelog(
          s"accepted. ${effects.createdContracts.size} created and ${effects.consumedContracts.size} contracts consumed."
        )

        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setTransactionEntry(txEntry)
            .build,
          stateUpdates.toMap
        )
    }
  }

  // ------------------------------------------------------

  private def buildRejectionLogEntry(
      recordTime: Timestamp,
      txEntry: DamlTransactionEntry,
      reason: RejectionReason): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {

    val rejectionEntry = {
      val builder = DamlRejectionEntry.newBuilder
      builder
        .setSubmitterInfo(txEntry.getSubmitterInfo)

      reason match {
        case RejectionReason.Inconsistent =>
          builder.setInconsistent("")
        case RejectionReason.Disputed(disputeReason) =>
          builder.setDisputed(disputeReason)
        case RejectionReason.ResourcesExhausted =>
          builder.setResourcesExhausted("")
        case RejectionReason.MaximumRecordTimeExceeded =>
          builder.setMaximumRecordTimeExceeded("")
        case RejectionReason.DuplicateCommand =>
          builder.setDuplicateCommand("")
        case RejectionReason.PartyNotKnownOnLedger =>
          builder.setPartyNotKnownOnLedger("")
        case RejectionReason.SubmitterCannotActViaParticipant(details) =>
          builder.setSubmitterCannotActViaParticipant(details)
      }
      builder.build
    }

    (
      DamlLogEntry.newBuilder
        .setRecordTime(buildTimestamp(recordTime))
        .setRejectionEntry(rejectionEntry)
        .build,
      Map.empty
    )
  }

  /** DamlStateValue presenting the empty value. Used for command deduplication entries. */
  private val commandDedupValue: DamlStateValue =
    DamlStateValue.newBuilder
      .setCommandDedup(DamlCommandDedupValue.newBuilder.build)
      .build

  /** Look up the contract instance from the log entry containing the transaction.
    *
    * This currently looks up the contract instance from the transaction stored
    * in the log entry, which is inefficient as it needs to decode the full transaction
    * to access a single contract instance.
    *
    * See issue https://github.com/digital-asset/daml/issues/734 for future work
    * to use a more efficient representation for transactions and contract instances.
    */
  private def lookupContractInstanceFromLogEntry(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      nodeId: Int): Option[ContractInst[Transaction.Value[AbsoluteContractId]]] = {
    val relTx = Conversions.decodeTransaction(entry.getTransactionEntry.getTransaction)
    relTx.nodes
      .get(NodeId.unsafeFromIndex(nodeId))
      .orElse {
        throw Err.NodeMissingFromLogEntry(entryId, nodeId)
      }
      .flatMap { node: Transaction.Node =>
        node match {
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            Some(
              create.coinst.mapValue(
                _.mapContractId(toAbsCoid(entryId, _))
              )
            )
          case n =>
            throw Err.NodeNotACreate(entryId, nodeId)
        }
      }
  }

}
