package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1.{Configuration, PackageId, RejectionReason}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.Node.NodeCreate
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  NodeId,
  VersionedValue
}
import com.google.protobuf.ByteString

object KeyValueCommitting {

  def packDamlStateKey(key: DamlStateKey): ByteString = key.toByteString
  def unpackDamlStateKey(bytes: ByteString): DamlStateKey = DamlStateKey.parseFrom(bytes)

  def packDamlStateValue(value: DamlStateValue): ByteString = value.toByteString
  def unpackDamlStateValue(bytes: ByteString): DamlStateValue = DamlStateValue.parseFrom(bytes)

  /** Processes a DAML submission, given the allocated log entry id, the submission and its resolved inputs.
    * Produces the log entry to be committed, and DAML state updates.
    *
    * The caller is expected to resolve the inputs declared in [[DamlSubmission]] prior
    * to calling this method, e.g. by reading [[DamlSubmission.getInputEntriesList]] and
    * [[DamlSubmission.getInputStateList]]
    *
    * The caller is expected to store the produced [[DamlLogEntry]] in key-value store at a location
    * that can be accessed through `entryId`. The DAML state updates may create new entries or update
    * existing entries in the key-value store. The concrete key for DAML state entry is obtained by applying
    * [[packDamlStateKey]] to [[DamlStateKey]].
    *
    * @param engine: The DAML Engine. This instance should be persistent as it caches package compilation.
    * @param config: The ledger configuration.
    * @param entryId: The log entry id to which this submission is committed.
    * @param recordTime: The record time for the log entry.
    * @param submission: The submission to commit to the ledger.
    * @param inputLogEntries: The resolved inputs to the submission.
    * @param inputState: The input DAML state entries.
    * @return The log entry to be committed and the DAML state updates.
    */
  def processSubmission(
      engine: Engine,
      config: Configuration,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      submission: DamlSubmission,
      inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
      inputState: Map[DamlStateKey, DamlStateValue]
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {

    submission.getPayloadCase match {
      case DamlSubmission.PayloadCase.ARCHIVE =>
        val archive = submission.getArchive
        val key = DamlStateKey.newBuilder.setPackageId(archive.getHash).build
        (
          DamlLogEntry.newBuilder
            .setRecordTime(buildTimestamp(recordTime))
            .setArchive(archive)
            .build,
          Map(
            key -> DamlStateValue.newBuilder.setArchiveEntry(entryId).build
          )
        )
      case DamlSubmission.PayloadCase.CONFIGURATION_ENTRY =>
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
        throw new RuntimeException("DamlSubmission payload not set!")
    }
  }

  private def processTransactionSubmission(
      engine: Engine,
      config: Configuration,
      entryId: DamlLogEntryId,
      recordTime: Timestamp,
      txEntry: DamlTransactionEntry,
      inputLogEntries: Map[DamlLogEntryId, DamlLogEntry],
      inputState: Map[DamlStateKey, DamlStateValue]
  ): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {

    // FIXME(JM): Dispatch on the submission type! The following is actually
    // "validateTransactionSubmission"
    val txLet = parseTimestamp(txEntry.getLedgerEffectiveTime)

    // 1. Verify that this is not a duplicate command submission.
    val dedupCheckResult = true

    // 2. Verify that the ledger effective time falls within time bounds
    val letCheckResult = config.timeModel.checkLet(
      currentTime = recordTime.toInstant,
      givenLedgerEffectiveTime = txLet.toInstant,
      givenMaximumRecordTime =
        parseTimestamp(txEntry.getSubmitterInfo.getMaximumRecordTime).toInstant
    )

    // 3. Verify that the input contracts are active.
    // FIXME(JM): Does this need to be done separately, or is it enough to
    // have lookupContract?
    val activenessCheckResult = true

    // 4. Verify that the submission conforms to the DAML model
    def lookupContract(coid: AbsoluteContractId) = {
      val (eid, nid) = absoluteContractIdToLogEntryId(coid)
      val stateKey = absoluteContractIdToStateKey(coid)
      for {
        contractState <- inputState.get(stateKey).map(_.getContractState)
        if txLet > parseTimestamp(contractState.getActiveAt) && !contractState.hasActiveAt
        entry <- inputLogEntries.get(eid)
        coinst <- lookupContractInstanceFromLogEntry(eid, entry, nid)
      } yield (coinst)
    }

    // FIXME(JM): Currently just faking this as we don't have the package inputs computed yet.
    // The provided engine is assumed to have all packages loaded.
    def lookupPackage(pkgId: PackageId) = sys.error("lookupPackage unimplemented")

    val relTx = Conversions.decodeTransaction(txEntry.getTransaction)
    val modelCheckResult = engine
      .validate(relTx, txLet)
      .consume(lookupContract, lookupPackage, _ => sys.error("unimplemented"))

    def reject(reason: RejectionReason): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) =
      (
        DamlLogEntry.newBuilder
          .setRecordTime(buildTimestamp(recordTime))
          .setRejectionEntry(
            buildRejectionEntry(txEntry, RejectionReason.DuplicateCommand)
          )
          .build,
        Map.empty
      )

    // FIXME(JM): Refactor this?
    (dedupCheckResult, letCheckResult, activenessCheckResult, modelCheckResult) match {
      case (false, _, _, _) =>
        reject(RejectionReason.DuplicateCommand)

      case (_, false, _, _) =>
        reject(RejectionReason.MaximumRecordTimeExceeded)

      case (_, _, false, _) =>
        reject(RejectionReason.Inconsistent)

      case (true, true, true, Left(err)) =>
        reject(RejectionReason.Disputed(err.msg)) // FIXME(JM): or detailMsg?

      case (true, true, true, Right(())) =>
        // All checks passed. Return transaction log entry, and update the DAML state
        // with the committed command and the created and consumed contracts.

        var stateUpdates = scala.collection.mutable.Map.empty[DamlStateKey, DamlStateValue]
        stateUpdates += commandDedupKey(txEntry) -> emptyDamlStateValue
        val effects = InputsAndEffects.computeEffects(entryId, relTx)
        effects.consumedContracts.foreach { key =>
          val cs = inputState(key).getContractState.toBuilder
          cs.setArchivedAt(buildTimestamp(recordTime))
          cs.setArchivedByEntry(entryId)
          stateUpdates += key -> DamlStateValue.newBuilder.setContractState(cs.build).build
        }
        effects.createdContracts.foreach { key =>
          val cs = DamlContractState.newBuilder
          cs.setActiveAt(buildTimestamp(recordTime))
          stateUpdates += key -> DamlStateValue.newBuilder.setContractState(cs).build
        }

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

  private def buildRejectionEntry(
      txEntry: DamlTransactionEntry,
      reason: RejectionReason): DamlRejectionEntry = {
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

  private val emptyDamlStateValue: DamlStateValue =
    DamlStateValue.newBuilder
      .setEmpty(com.google.protobuf.Empty.newBuilder.build)
      .build

  private def commandDedupKey(txEntry: DamlTransactionEntry): DamlStateKey = {
    val subInfo = txEntry.getSubmitterInfo
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

  /** Look up the contract instance from the log entry containing the transaction.
    */
  private def lookupContractInstanceFromLogEntry(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      nodeId: Int): Option[ContractInst[Transaction.Value[AbsoluteContractId]]] = {
    val relTx = Conversions.decodeTransaction(entry.getTransactionEntry.getTransaction)
    relTx.nodes
      .get(NodeId.unsafeFromIndex(nodeId))
      .flatMap { (node: Transaction.Node) =>
        node match {
          case create: NodeCreate[ContractId, VersionedValue[ContractId]] =>
            Some(
              create.coinst.mapValue(
                _.mapContractId(toAbsCoid(entryId, _))
              )
            )
          case _ =>
            // TODO(JM): Logging?
            None
        }
      }
  }

}
