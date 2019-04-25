package com.daml.ledger.participant.state.kvutils

import java.time.{Duration, Instant}

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.Node.{
  NodeCreate,
  NodeExercises,
  NodeFetch,
  NodeLookupByKey
}
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value.{
  AbsoluteContractId,
  ContractId,
  ContractInst,
  NodeId,
  RelativeContractId,
  VersionedValue
}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.services.time.TimeModel
import com.google.common.io.BaseEncoding
import com.google.protobuf.ByteString

/**
  * Utilities for implementing participant state on top of a key-value based ledger.
  * Implements helpers and serialization/deserialization into protocol buffer messages
  * defined in daml_kvutils.proto.
  */
object KeyValueUtils {

  /** The effects of the transaction, that is what contracts
    * were consumed and created, and what contract keys were updated.
    */
  case class Effects(
      /** The contracts consumed by this transaction.
        * When committing the transaction these contracts must be marked consumed.
        * A contract should be marked consumed when the transaction is committed,
        * regardless of the ledger effective time of the transaction (e.g. a transaction
        * with an earlier ledger effective time that gets committed later would find the
        * contract inactive).
        */
      consumedContracts: List[DamlStateKey],
      /** The contracts created by this transaction.
        * When the transaction is committed, keys marking the activeness of these
        * contracts should be created. The key should be a combination of the transaction
        * id and the relative contract id (that is, the node index).
        */
      createdContracts: List[DamlStateKey]

      // FIXME(JM): updated contract keys
  )

  /** Transform the submitted transaction into entry blob. */
  def transactionToSubmission(
      submitterInfo: SubmitterInfo,
      meta: TransactionMeta,
      tx: SubmittedTransaction): DamlSubmission = {
    DamlSubmission.newBuilder
      .setTransactionEntry(
        DamlTransactionEntry.newBuilder
          .setTransaction(TransactionCoding.encodeTransaction(tx))
          .setSubmitterInfo(buildSubmitterInfo(submitterInfo))
          .setLedgerEffectiveTime(buildTimestamp(meta.ledgerEffectiveTime))
          .setWorkflowId(meta.workflowId)
          .build
      )
      .build
  }

  def archiveToSubmission(archive: Archive): DamlSubmission = {
    DamlSubmission.newBuilder
      .setArchive(archive)
      .build
  }

  def configurationToSubmission(config: Configuration): DamlSubmission = {
    val tm = config.timeModel
    DamlSubmission.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setTimeModel(
            DamlTimeModel.newBuilder
              .setMaxClockSkew(buildDuration(tm.maxClockSkew))
              .setMinTransactionLatency(buildDuration(tm.minTransactionLatency))
              .setMaxTtl(buildDuration(tm.maxTtl))
          )
      )
      .build
  }

  def decodeDamlConfigurationEntry(config: DamlConfigurationEntry): Configuration = {
    val tm = config.getTimeModel
    Configuration(
      TimeModel(
        maxClockSkew = parseDuration(tm.getMaxClockSkew),
        minTransactionLatency = parseDuration(tm.getMinTransactionLatency),
        maxTtl = parseDuration(tm.getMaxTtl)
      ).get // FIXME(JM): handle error
    )
  }

  /** Convert the entry blob into a participant state [[Update]].
    * The caller is expected to provide the record time of the batch into which the entry
    * was committed and to produce the accompanying [[Offset]] from the entryId.
    *
    * @param entryId: The transaction identifier assigned to the transaction.
    * @param entry: The entry blob.
    * @param recordTime: The record time of the batch into which the entry was committed.
    * @param lookupFromState: Function to look up values from state.
    * @return An update, or None if a lookup from state fails.
    *
    * // FIXME(JM): Might need futures for the lookup?
    */
  def logEntryToUpdate(
      entryId: DamlLogEntryId,
      entry: DamlLogEntry,
      recordTime: Timestamp,
      lookupFromState: DamlStateKey => Option[DamlStateValue]): Option[Update] = {
    entry.getPayloadCase match {
      case DamlLogEntry.PayloadCase.PACKAGE_MARK =>
        lookupFromState(entry.getPackageMark).map { v =>
          Update.PublicPackageUploaded(v.getArchive)
        }

      case DamlLogEntry.PayloadCase.TRANSACTION_ENTRY =>
        Some(txEntryToUpdate(entryId, entry.getTransactionEntry, recordTime))

      case DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY =>
        Some(Update.ConfigurationChanged(decodeDamlConfigurationEntry(entry.getConfigurationEntry)))

      case DamlLogEntry.PayloadCase.REJECTION_ENTRY =>
        Some(rejEntryToUpdate(entryId, entry.getRejectionEntry, recordTime))

      case DamlLogEntry.PayloadCase.PAYLOAD_NOT_SET =>
        throw new RuntimeException("entryToUpdate: Payload is not set!")
    }
  }

  private def rejEntryToUpdate(
      entryId: DamlLogEntryId,
      rejEntry: DamlRejectionEntry,
      recordTime: Timestamp): Update.CommandRejected = {

    Update.CommandRejected(
      submitterInfo = parseSubmitterInfo(rejEntry.getSubmitterInfo),
      reason = RejectionReason.Inconsistent // FIXME
    )
  }

  private def buildSubmitterInfo(subInfo: SubmitterInfo): DamlSubmitterInfo =
    DamlSubmitterInfo.newBuilder
      .setSubmitter(subInfo.submitter.underlyingString)
      .setApplicationId(subInfo.applicationId)
      .setCommandId(subInfo.commandId)
      .setMaximumRecordTime(buildTimestamp(subInfo.maxRecordTime))
      .build

  private def parseSubmitterInfo(subInfo: DamlSubmitterInfo): SubmitterInfo =
    SubmitterInfo(
      submitter = SimpleString.assertFromString(subInfo.getSubmitter),
      applicationId = subInfo.getApplicationId,
      commandId = subInfo.getCommandId,
      maxRecordTime = parseTimestamp(subInfo.getMaximumRecordTime)
    )

  /** Transform the transaction entry into the [[Update.TransactionAccepted]] event. */
  private def txEntryToUpdate(
      entryId: DamlLogEntryId,
      txEntry: DamlTransactionEntry,
      recordTime: Timestamp): Update.TransactionAccepted = {
    val relTx = TransactionCoding.decodeTransaction(txEntry.getTransaction)
    val hexTxId = BaseEncoding.base16.encode(entryId.toByteArray)

    Update.TransactionAccepted(
      optSubmitterInfo = Some(parseSubmitterInfo(txEntry.getSubmitterInfo)),
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = parseTimestamp(txEntry.getLedgerEffectiveTime),
        workflowId = txEntry.getWorkflowId,
      ),
      transaction = makeCommittedTransaction(entryId, relTx),
      transactionId = hexTxId,
      recordTime = recordTime,
      referencedContracts = List.empty // TODO(JM): rename this to additionalContracts. Always empty here.
    )
  }

  // ------------------------------------------------------

  private def computeInputs(
      tx: SubmittedTransaction): (List[DamlLogEntryId], List[DamlStateKey]) = {
    // FIXME(JM): Get referenced packages from the transaction (once they're added to it)
    def addInput(inputs: List[DamlLogEntryId], coid: ContractId): List[DamlLogEntryId] =
      coid match {
        case acoid: AbsoluteContractId =>
          absoluteContractIdToLogEntryId(acoid)._1 :: inputs
        case _ =>
          inputs
      }

    tx.fold(GenTransaction.TopDown, (List.empty[DamlLogEntryId], List.empty[DamlStateKey])) {
      case ((logEntryInputs, stateInputs), (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            (addInput(logEntryInputs, fetch.coid), stateInputs)
          case create: NodeCreate[_, _] =>
            (logEntryInputs, stateInputs)
          case exe: NodeExercises[_, ContractId, _] =>
            (
              addInput(logEntryInputs, exe.targetCoid),
              (exe.consuming, exe.targetCoid) match {
                case (true, acoid: AbsoluteContractId) =>
                  absoluteContractIdToStateKey(acoid) :: stateInputs
                case _ =>
                  stateInputs
              }
            )
          case l: NodeLookupByKey[_, _] =>
            // FIXME(JM): track fetched keys
            (logEntryInputs, stateInputs)
        }
    }
  }

  private def computeEffects(entryId: DamlLogEntryId, tx: SubmittedTransaction): Effects = {
    tx.fold(GenTransaction.TopDown, Effects(List.empty, List.empty)) {
      case (effects, (nodeId, node)) =>
        node match {
          case fetch: NodeFetch[ContractId] =>
            effects
          case create: NodeCreate[_, _] =>
            // FIXME(JM): Track created keys
            effects.copy(
              createdContracts =
                relativeContractIdToStateKey(entryId, create.coid.asInstanceOf[RelativeContractId])
                  :: effects.createdContracts
            )
          case exe: NodeExercises[_, ContractId, _] =>
            if (exe.consuming) {
              exe.targetCoid match {
                case acoid: AbsoluteContractId =>
                  effects.copy(
                    consumedContracts = absoluteContractIdToStateKey(acoid) :: effects.consumedContracts
                  )
                case _ =>
                  effects
              }
            } else {
              effects
            }
          case l: NodeLookupByKey[_, _] =>
            effects
        }
    }
  }

  private def toAbsCoid(txId: DamlLogEntryId, coid: ContractId): AbsoluteContractId = {
    val hexTxId =
      BaseEncoding.base16.encode(txId.toByteArray)
    coid match {
      case a @ AbsoluteContractId(_) => a
      case RelativeContractId(txnid) =>
        // NOTE(JM): Must match with decodeAbsoluteContractId
        AbsoluteContractId(s"$hexTxId:${txnid.index}")
    }
  }

  private def makeCommittedTransaction(
      txId: DamlLogEntryId,
      tx: SubmittedTransaction): CommittedTransaction = {
    tx
    /* Assign absolute contract ids */
      .mapContractIdAndValue(
        toAbsCoid(txId, _),
        _.mapContractId(toAbsCoid(txId, _))
      )
  }

  private def absoluteContractIdToLogEntryId(acoid: AbsoluteContractId): (DamlLogEntryId, Int) =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        DamlLogEntryId.newBuilder
          .setEntryIdBytes(ByteString.copyFrom(BaseEncoding.base16().decode(hexTxId)))
          .build -> nodeId.toInt
      case _ => sys.error(s"decodeAbsoluteContractId: Cannot decode '$acoid'")
    }

  private def absoluteContractIdToStateKey(acoid: AbsoluteContractId): DamlStateKey =
    acoid.coid.split(':').toList match {
      case hexTxId :: nodeId :: Nil =>
        DamlStateKey.newBuilder
          .setContractId(
            DamlContractId.newBuilder
              .setEntryId(DamlLogEntryId.newBuilder.setEntryIdBytes(
                ByteString.copyFrom(BaseEncoding.base16().decode(hexTxId))))
              .setNodeId(nodeId.toLong)
              .build
          )
          .build
      case _ => sys.error(s"decodeAbsoluteContractId: Cannot decode '$acoid'")
    }

  private def relativeContractIdToStateKey(
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

  private def buildTimestamp(ts: Time.Timestamp): com.google.protobuf.Timestamp = {
    val instant = ts.toInstant
    com.google.protobuf.Timestamp.newBuilder
      .setSeconds(instant.getEpochSecond)
      .setNanos(instant.getNano)
      .build
  }

  private def parseTimestamp(ts: com.google.protobuf.Timestamp): Time.Timestamp =
    Time.Timestamp.assertFromInstant(Instant.ofEpochSecond(ts.getSeconds, ts.getNanos.toLong))

  private def buildDuration(dur: Duration): com.google.protobuf.Duration = {
    com.google.protobuf.Duration.newBuilder
      .setSeconds(dur.getSeconds)
      .setNanos(dur.getNano)
      .build
  }

  private def parseDuration(dur: com.google.protobuf.Duration): Duration = {
    Duration.ofSeconds(dur.getSeconds, dur.getNanos.toLong)
  }

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

  // ------------------------------------------
  /** Validates a submission, given the submission and its inputs.
    * Produces a list of log entries to be committed, and new DAML state entries to be
    * created/updated.
    *
    * @param engine: The DAML Engine. This instance should be persistent as it caches package compilation.
    * @param config: The ledger configuration.
    * @param entryId: The log entry id to which this submission is committed.
    * @param recordTime: The record time for the log entry.
    * @param submission: The submission to commit to the ledger.
    * @param inputs: The resolved inputs to the submission.
    * @return The log entries to be committed, and the state to be created/updated.
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
            .setPackageMark(key)
            .build,
          Map(
            key -> DamlStateValue.newBuilder.setArchive(archive).build
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

    val relTx = TransactionCoding.decodeTransaction(txEntry.getTransaction)
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
        val effects = computeEffects(entryId, relTx)
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
    val relTx = TransactionCoding.decodeTransaction(entry.getTransactionEntry.getTransaction)
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
