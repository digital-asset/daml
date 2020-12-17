// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.time.Instant

import com.codahale.metrics.Counter
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.committer.TransactionCommitter._
import com.daml.ledger.participant.state.kvutils.{Conversions, Err, InputsAndEffects}
import com.daml.ledger.participant.state.v1.{Configuration, RejectionReason, TimeModel}
import com.daml.lf.archive.Decode
import com.daml.lf.archive.Reader.ParseError
import com.daml.lf.crypto
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Blinding, Engine, ReplayMismatch}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{
  BlindingInfo,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  ReplayNodeMismatch,
  SubmittedTransaction,
  TransactionOuterClass,
  VersionedTransaction,
  Transaction => Tx
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.metrics.Metrics
import com.google.protobuf.{Timestamp => ProtoTimestamp}

import scala.collection.JavaConverters._

// The parameter inStaticTimeMode indicates that the ledger is running in static time mode.
//
// Command deduplication is always based on wall clock time and not ledger time. In static time mode,
// record time cannot be used for command deduplication. This flag indicates that the system clock should
// be used as submission time for commands instead of record time.
//
// Other possible solutions that we discarded:
// * Pass in an additional time provider, but this hides the intent
// * Adding and additional submission field commandDedupSubmissionTime field. While having participants
//   provide this field *could* lead to possible exploits, they are not exploits that could do any harm.
//   The bigger concern is adding a public API for the specific use case of Sandbox with static time.
private[kvutils] class TransactionCommitter(
    defaultConfig: Configuration,
    engine: Engine,
    override protected val metrics: Metrics,
    inStaticTimeMode: Boolean
) extends Committer[DamlTransactionEntrySummary] {
  override protected val committerName = "transaction"

  override protected def init(
      commitContext: CommitContext,
      submission: DamlSubmission,
  ): DamlTransactionEntrySummary =
    DamlTransactionEntrySummary(submission.getTransactionEntry)

  override protected val steps: Iterable[(StepInfo, Step)] = Iterable(
    "authorize_submitter" -> authorizeSubmitters,
    "check_informee_parties_allocation" -> checkInformeePartiesAllocation,
    "deduplicate" -> deduplicateCommand,
    "validate_ledger_time" -> validateLedgerTime,
    "validate_contract_keys" -> validateContractKeys,
    "validate_model_conformance" -> validateModelConformance,
    "blind" -> blind,
    "trim_unnecessary_nodes" -> trimUnnecessaryNodes,
    "build_final_log_entry" -> buildFinalLogEntry,
  )

  private def contractIsActive(
      transactionEntry: DamlTransactionEntrySummary,
      contractState: DamlContractState,
  ): Boolean = {
    val activeAt = Option(contractState.getActiveAt).map(parseTimestamp)
    !contractState.hasArchivedAt && activeAt.exists(transactionEntry.ledgerEffectiveTime >= _)
  }

  /** Reject duplicate commands
    */
  private[committer] def deduplicateCommand: Step = (commitContext, transactionEntry) => {
    commitContext.recordTime
      .map { recordTime =>
        val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
        val dedupEntry = commitContext.get(dedupKey)
        val submissionTime =
          if (inStaticTimeMode) Instant.now() else recordTime.toInstant
        if (dedupEntry.forall(isAfterDeduplicationTime(submissionTime, _))) {
          StepContinue(transactionEntry)
        } else {
          logger.trace(
            s"Transaction rejected, duplicate command, correlationId=${transactionEntry.commandId}")
          reject(
            commitContext.recordTime,
            DamlTransactionRejectionEntry.newBuilder
              .setSubmitterInfo(transactionEntry.submitterInfo)
              .setDuplicateCommand(Duplicate.newBuilder.setDetails(""))
          )
        }
      }
      .getOrElse(StepContinue(transactionEntry))
  }

  // Checks that the submission time of the command is after the
  // deduplicationTime represented by stateValue
  private def isAfterDeduplicationTime(
      submissionTime: Instant,
      stateValue: DamlStateValue): Boolean = {
    val cmdDedup = stateValue.getCommandDedup
    if (stateValue.hasCommandDedup && cmdDedup.hasDeduplicatedUntil) {
      val dedupTime = parseTimestamp(cmdDedup.getDeduplicatedUntil).toInstant
      dedupTime.isBefore(submissionTime)
    } else {
      false
    }
  }

  /** Authorize the submission by looking up the party allocation and verifying
    * that all of the submitting parties are indeed hosted by the submitting participant.
    */
  private[committer] def authorizeSubmitters: Step = (commitContext, transactionEntry) => {
    def rejection(reason: RejectionReason) =
      reject[DamlTransactionEntrySummary](
        commitContext.recordTime,
        buildRejectionLogEntry(transactionEntry, reason))

    @scala.annotation.tailrec
    def authorizeAll(submitters: List[Party]): StepResult[DamlTransactionEntrySummary] =
      submitters match {
        case Nil =>
          StepContinue(transactionEntry)
        case submitter :: others =>
          authorize(submitter) match {
            case Some(rejection) =>
              rejection
            case None =>
              authorizeAll(others)
          }
      }

    def authorize(submitter: Party): Option[StepResult[DamlTransactionEntrySummary]] =
      commitContext.get(partyStateKey(submitter)) match {
        case Some(partyAllocation)
            if partyAllocation.getParty.getParticipantId == commitContext.participantId =>
          None
        case Some(_) =>
          Some(
            rejection(
              RejectionReason.SubmitterCannotActViaParticipant(
                s"Party '$submitter' not hosted by participant ${commitContext.participantId}")
            ))
        case None =>
          Some(
            rejection(
              RejectionReason.PartyNotKnownOnLedger(s"Submitting party '$submitter' not known")
            ))
      }

    authorizeAll(transactionEntry.submitters)
  }

  /** Validate ledger effective time and the command's time-to-live. */
  private[committer] def validateLedgerTime: Step =
    (commitContext, transactionEntry) => {
      val (_, config) = getCurrentConfiguration(defaultConfig, commitContext, logger)
      val timeModel = config.timeModel

      commitContext.recordTime match {
        case Some(recordTime) =>
          val givenLedgerTime = transactionEntry.ledgerEffectiveTime.toInstant

          timeModel
            .checkTime(ledgerTime = givenLedgerTime, recordTime = recordTime.toInstant)
            .fold(
              reason =>
                reject(
                  commitContext.recordTime,
                  buildRejectionLogEntry(
                    transactionEntry,
                    RejectionReason.InvalidLedgerTime(reason))),
              _ => StepContinue(transactionEntry)
            )
        case None => // Pre-execution: propagate the time bounds and defer the checks to post-execution.
          val maybeDeduplicateUntil =
            getLedgerDeduplicateUntil(transactionEntry, commitContext)
          val minimumRecordTime = transactionMinRecordTime(
            transactionEntry.submissionTime.toInstant,
            transactionEntry.ledgerEffectiveTime.toInstant,
            maybeDeduplicateUntil,
            timeModel)
          val maximumRecordTime = transactionMaxRecordTime(
            transactionEntry.submissionTime.toInstant,
            transactionEntry.ledgerEffectiveTime.toInstant,
            timeModel)
          commitContext.deduplicateUntil = maybeDeduplicateUntil
          commitContext.minimumRecordTime = Some(minimumRecordTime)
          commitContext.maximumRecordTime = Some(maximumRecordTime)
          val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
            .setTransactionRejectionEntry(
              buildRejectionLogEntry(
                transactionEntry,
                RejectionReason.InvalidLedgerTime(
                  s"Record time is outside of valid range [$minimumRecordTime, $maximumRecordTime]")
              )
            )
            .build
          commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
          StepContinue(transactionEntry)
      }
    }

  /** Validate the submission's conformance to the DAML model */
  private def validateModelConformance: Step =
    (commitContext, transactionEntry) =>
      metrics.daml.kvutils.committer.transaction.interpretTimer.time(() => {
        // Pull all keys from referenced contracts. We require this for 'fetchByKey' calls
        // which are not evidenced in the transaction itself and hence the contract key state is
        // not included in the inputs.
        lazy val knownKeys: Map[DamlContractKey, Value.ContractId] =
          commitContext.collectInputs {
            case (key, Some(value))
                if value.getContractState.hasContractKey
                  && contractIsActive(transactionEntry, value.getContractState) =>
              value.getContractState.getContractKey -> Conversions.stateKeyToContractId(key)
          }

        try {
          engine
            .validate(
              transactionEntry.submitters.toSet,
              SubmittedTransaction(transactionEntry.transaction),
              transactionEntry.ledgerEffectiveTime,
              commitContext.participantId,
              transactionEntry.submissionTime,
              transactionEntry.submissionSeed,
            )
            .consume(
              lookupContract(transactionEntry, commitContext),
              lookupPackage(transactionEntry, commitContext),
              lookupKey(commitContext, knownKeys),
            )
            .fold(
              err =>
                reject[DamlTransactionEntrySummary](
                  commitContext.recordTime,
                  buildRejectionLogEntry(transactionEntry, rejectionReasonForValidationError(err))),
              _ => StepContinue[DamlTransactionEntrySummary](transactionEntry)
            )
        } catch {
          case err: Err.MissingInputState =>
            logger.warn("Exception during model conformance validation.", err)
            reject(
              commitContext.recordTime,
              buildRejectionLogEntry(transactionEntry, RejectionReason.Disputed(err.getMessage)))
        }
      })

  private[committer] def rejectionReasonForValidationError(
      validationError: com.daml.lf.engine.Error): RejectionReason = {
    def disputed: RejectionReason = RejectionReason.Disputed(validationError.msg)

    def resultIsCreatedInTx(
        tx: VersionedTransaction[NodeId, ContractId],
        result: Option[Value.ContractId]): Boolean =
      result.exists { contractId =>
        tx.nodes.exists {
          case (nodeId @ _, create: Node.NodeCreate[_]) => create.coid == contractId
          case _ => false
        }
      }

    validationError match {
      case ReplayMismatch(
          ReplayNodeMismatch(recordedTx, recordedNodeId, replayedTx, replayedNodeId)) =>
        // If the problem is that a key lookup has changed and the results do not involve contracts created in this transaction,
        // then it's a consistency problem.

        (recordedTx.nodes(recordedNodeId), replayedTx.nodes(replayedNodeId)) match {
          case (
              Node.NodeLookupByKey(
                recordedTemplateId,
                recordedOptLocation @ _,
                recordedKey,
                recordedResult,
                recordedVersion,
              ),
              Node.NodeLookupByKey(
                replayedTemplateId,
                replayedOptLocation @ _,
                replayedKey,
                replayedResult,
                replayedVersion,
              ))
              if recordedVersion == replayedVersion &&
                recordedTemplateId == replayedTemplateId && recordedKey == replayedKey
                && !resultIsCreatedInTx(recordedTx, recordedResult)
                && !resultIsCreatedInTx(replayedTx, replayedResult) =>
            RejectionReason.Inconsistent(validationError.msg)
          case _ => disputed
        }
      case _ => disputed
    }
  }

  /** Validate the submission's conformance to the DAML model */
  private[committer] def blind: Step =
    (commitContext, transactionEntry) => {
      val blindingInfo = Blinding.blind(transactionEntry.transaction)
      setDedupEntryAndUpdateContractState(
        commitContext,
        transactionEntry.copy(
          submission = transactionEntry.submission.toBuilder
            .setBlindingInfo(Conversions.encodeBlindingInfo(blindingInfo))
            .build),
        blindingInfo,
      )
    }

  /**
    * Removes `Fetch` and `LookupByKey` nodes from the transactionEntry.
    */
  private[committer] def trimUnnecessaryNodes: Step = (_, transactionEntry) => {
    val transaction = transactionEntry.submission.getTransaction
    val nodes = transaction.getNodesList.asScala
    val nodesToKeep = nodes.iterator.collect {
      case node if node.hasCreate || node.hasExercise => node.getNodeId
    }.toSet

    val filteredRoots = transaction.getRootsList.asScala.filter(nodesToKeep)

    def stripUnnecessaryNodes(node: TransactionOuterClass.Node) =
      if (node.hasExercise) {
        val exerciseNode = node.getExercise
        val keptChildren = exerciseNode.getChildrenList.asScala.filter(nodesToKeep)
        val newExerciseNode = exerciseNode.toBuilder
          .clearChildren()
          .addAllChildren(keptChildren.asJavaCollection)
          .build()

        node.toBuilder
          .setExercise(newExerciseNode)
          .build()
      } else {
        node
      }

    val filteredNodes = nodes
      .collect {
        case node if nodesToKeep(node.getNodeId) => stripUnnecessaryNodes(node)
      }

    val newTransaction = transaction
      .newBuilderForType()
      .addAllRoots(filteredRoots.asJavaCollection)
      .addAllNodes(filteredNodes.asJavaCollection)
      .setVersion(transaction.getVersion)
      .build()

    val newTransactionEntry = transactionEntry.submission.toBuilder
      .setTransaction(newTransaction)
      .build()

    StepContinue(DamlTransactionEntrySummary(newTransactionEntry))
  }

  /**
    * Builds the log entry as the final step.
    */
  private def buildFinalLogEntry: Step =
    (commitContext, transactionEntry) => StepStop(buildLogEntry(transactionEntry, commitContext))

  private def validateContractKeys: Step = (commitContext, transactionEntry) => {
    val damlState = commitContext.collectInputs {
      case (key, Some(value)) if key.hasContractKey => key -> value
    } ++ commitContext.getOutputs
    val startingKeys = damlState.collect {
      case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty => k
    }.toSet
    validateContractKeyUniqueness(commitContext.recordTime, transactionEntry, startingKeys) match {
      case StepContinue(transactionEntry) =>
        validateContractKeyCausalMonotonicity(
          commitContext.recordTime,
          transactionEntry,
          startingKeys,
          damlState)
      case err => err
    }
  }

  private def validateContractKeyUniqueness(
      recordTime: Option[Timestamp],
      transactionEntry: DamlTransactionEntrySummary,
      keys: Set[DamlStateKey]): StepResult[DamlTransactionEntrySummary] = {
    val allUnique = transactionEntry.transaction
      .fold((true, keys)) {
        case ((allUnique, existingKeys), (_, exe: Node.NodeExercises[NodeId, Value.ContractId]))
            if exe.key.isDefined && exe.consuming =>
          val stateKey = Conversions.globalKeyToStateKey(
            GlobalKey(exe.templateId, Conversions.forceNoContractIds(exe.key.get.key)))
          (allUnique, existingKeys - stateKey)

        case ((allUnique, existingKeys), (_, create: Node.NodeCreate[Value.ContractId]))
            if create.key.isDefined =>
          val stateKey = Conversions.globalKeyToStateKey(
            GlobalKey(create.coinst.template, Conversions.forceNoContractIds(create.key.get.key)))

          (allUnique && !existingKeys.contains(stateKey), existingKeys + stateKey)

        case (accum, _) => accum
      }
      ._1

    if (allUnique)
      StepContinue(transactionEntry)
    else
      reject(
        recordTime,
        buildRejectionLogEntry(
          transactionEntry,
          RejectionReason.Inconsistent("DuplicateKey: Contract Key not unique")))

  }

  /** LookupByKey nodes themselves don't actually fetch the contract.
    * Therefore we need to do an additional check on all contract keys
    * to ensure the referred contract satisfies the causal monotonicity invariant.
    * This could be reduced to only validate this for keys referred to by
    * NodeLookupByKey.
    */
  private def validateContractKeyCausalMonotonicity(
      recordTime: Option[Timestamp],
      transactionEntry: DamlTransactionEntrySummary,
      keys: Set[DamlStateKey],
      damlState: Map[DamlStateKey, DamlStateValue]): StepResult[DamlTransactionEntrySummary] = {
    val causalKeyMonotonicity = keys.forall { key =>
      val state = damlState(key)
      val keyActiveAt =
        Conversions.parseTimestamp(state.getContractKeyState.getActiveAt).toInstant
      !keyActiveAt.isAfter(transactionEntry.ledgerEffectiveTime.toInstant)
    }
    if (causalKeyMonotonicity)
      StepContinue(transactionEntry)
    else
      reject(
        recordTime,
        buildRejectionLogEntry(
          transactionEntry,
          RejectionReason.Inconsistent("Causal monotonicity violated")))
  }

  /** Check that all informee parties mentioned of a transaction are allocated. */
  private def checkInformeePartiesAllocation: Step = (commitContext, transactionEntry) => {
    def foldInformeeParties(tx: Tx.Transaction, init: Boolean)(
        f: (Boolean, String) => Boolean
    ): Boolean =
      tx.fold(init) {
        case (accum, (_, node)) =>
          node.informeesOfNode.foldLeft(accum)(f)
      }

    val allExist = foldInformeeParties(transactionEntry.transaction, init = true) {
      (accum, party) =>
        commitContext.get(partyStateKey(party)).fold(false)(_ => accum)
    }

    if (allExist)
      StepContinue(transactionEntry)
    else
      reject(
        commitContext.recordTime,
        buildRejectionLogEntry(
          transactionEntry,
          RejectionReason.PartyNotKnownOnLedger("Not all parties known"))
      )
  }

  /** Produce the log entry and contract state updates. */
  private def setDedupEntryAndUpdateContractState(
      commitContext: CommitContext,
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo
  ): StepResult[DamlTransactionEntrySummary] = {
    // Set a deduplication entry.
    commitContext.set(
      commandDedupKey(transactionEntry.submitterInfo),
      DamlStateValue.newBuilder
        .setCommandDedup(
          DamlCommandDedupValue.newBuilder
            .setDeduplicatedUntil(transactionEntry.submitterInfo.getDeduplicateUntil)
            .build)
        .build
    )

    updateContractState(transactionEntry, blindingInfo, commitContext)

    metrics.daml.kvutils.committer.transaction.accepts.inc()
    logger.trace(s"Transaction accepted, correlationId=${transactionEntry.commandId}")
    StepContinue(transactionEntry)
  }

  private def updateContractState(
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo,
      commitContext: CommitContext): Unit = {
    val effects = InputsAndEffects.computeEffects(transactionEntry.transaction)
    val cid2nid: Value.ContractId => NodeId =
      transactionEntry.transaction.localContracts
    // Add contract state entries to mark contract activeness (checked by 'validateModelConformance').
    for ((key, createNode) <- effects.createdContracts) {
      val cs = DamlContractState.newBuilder
      cs.setActiveAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
      val localDisclosure =
        blindingInfo.disclosure(cid2nid(decodeContractId(key.getContractId)))
      cs.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
      cs.setContractInstance(
        Conversions.encodeContractInstance(createNode.versionedCoinst)
      )
      createNode.key.foreach { keyWithMaintainers =>
        cs.setContractKey(
          Conversions.encodeGlobalKey(
            GlobalKey
              .build(
                createNode.coinst.template,
                keyWithMaintainers.key
              )
              .fold(
                _ =>
                  throw Err.InvalidSubmission("Contract IDs are not supported in contract keys."),
                identity))
        )
      }
      commitContext.set(key, DamlStateValue.newBuilder.setContractState(cs).build)
    }
    // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance').
    for (key <- effects.consumedContracts) {
      val cs = getContractState(commitContext, key)
      commitContext.set(
        key,
        DamlStateValue.newBuilder
          .setContractState(
            cs.toBuilder
              .setArchivedAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
          )
          .build
      )
    }
    // Update contract state of divulged contracts.
    for ((coid, parties) <- blindingInfo.divulgence) {
      val key = contractIdToStateKey(coid)
      val cs = getContractState(commitContext, key)
      val divulged: Set[String] = cs.getDivulgedToList.asScala.toSet
      val newDivulgences: Set[String] = parties.toSet[String] -- divulged
      if (newDivulgences.nonEmpty) {
        val cs2 = cs.toBuilder
          .addAllDivulgedTo(newDivulgences.asJava)
        commitContext.set(key, DamlStateValue.newBuilder.setContractState(cs2).build)
      }
    }
    // Update contract keys.
    val ledgerEffectiveTime = transactionEntry.submission.getLedgerEffectiveTime
    for ((key, contractKeyState) <- effects.updatedContractKeys) {
      val (k, v) =
        updateContractKeyWithContractKeyState(ledgerEffectiveTime, key, contractKeyState)
      commitContext.set(k, v)
    }
  }

  private[committer] def buildLogEntry(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext): DamlLogEntry = {
    if (commitContext.preExecute) {
      val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
        .setTransactionRejectionEntry(
          DamlTransactionRejectionEntry.newBuilder
            .setSubmitterInfo(transactionEntry.submitterInfo))
        .build
      commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
    }
    buildLogEntryWithOptionalRecordTime(
      commitContext.recordTime,
      _.setTransactionEntry(transactionEntry.submission))
  }

  private def updateContractKeyWithContractKeyState(
      ledgerEffectiveTime: ProtoTimestamp,
      key: DamlStateKey,
      contractKeyState: Option[ContractId]): (DamlStateKey, DamlStateValue) = {
    logger.trace(s"updating contract key $key to $contractKeyState")
    key ->
      DamlStateValue.newBuilder
        .setContractKeyState(
          contractKeyState
            .map(
              coid =>
                DamlContractKeyState.newBuilder
                  .setContractId(coid.coid)
                  .setActiveAt(ledgerEffectiveTime))
            .getOrElse(DamlContractKeyState.newBuilder())
        )
        .build
  }

  // Helper to lookup contract instances. We verify the activeness of
  // contract instances here. Since we look up every contract that was
  // an input to a transaction, we do not need to verify the inputs separately.
  private def lookupContract(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext)(
      coid: Value.ContractId,
  ): Option[Value.ContractInst[Value.VersionedValue[Value.ContractId]]] = {
    val stateKey = contractIdToStateKey(coid)
    for {
      // Fetch the state of the contract so that activeness can be checked.
      // There is the possibility that the reinterpretation of the transaction yields a different
      // result in a LookupByKey than the original transaction. This means that the contract state data for the
      // contractId pointed to by that contractKey might not have been preloaded into the input state map.
      // This is not a problem because after the transaction reinterpretation, we compare the original
      // transaction with the reinterpreted one, and the LookupByKey node will not match.
      // Additionally, all contract keys are checked to uphold causal monotonicity.
      contractState <- commitContext.read(stateKey).map(_.getContractState)
      if contractIsActive(transactionEntry, contractState)
      contract = Conversions.decodeContractInstance(contractState.getContractInstance)
    } yield contract
  }

  // Helper to lookup package from the state. The package contents
  // are stored in the [[DamlLogEntry]], which we find by looking up
  // the DAML state entry at `DamlStateKey(packageId = pkgId)`.
  private def lookupPackage(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
  )(pkgId: PackageId): Option[Ast.Package] = {
    val stateKey = packageStateKey(pkgId)
    for {
      value <- commitContext
        .read(stateKey)
        .orElse {
          logger.warn(
            s"Lookup package failed, package not found, packageId=$pkgId correlationId=${transactionEntry.commandId}")
          throw Err.MissingInputState(stateKey)
        }
      pkg <- value.getValueCase match {
        case DamlStateValue.ValueCase.ARCHIVE =>
          // NOTE(JM): Engine only looks up packages once, compiles and caches,
          // provided that the engine instance is persisted.
          try {
            Some(Decode.decodeArchive(value.getArchive)._2)
          } catch {
            case ParseError(err) =>
              logger.warn(
                s"Decode archive failed, packageId=$pkgId correlationId=${transactionEntry.commandId}")
              throw Err.DecodeError("Archive", err)
          }

        case _ =>
          val msg = s"value not a DAML-LF archive"
          logger.warn(
            s"Lookup package failed, $msg, packageId=$pkgId correlationId=${transactionEntry.commandId}")
          throw Err.DecodeError("Archive", msg)
      }

    } yield pkg
  }

  private def lookupKey(
      commitContext: CommitContext,
      knownKeys: Map[DamlContractKey, Value.ContractId],
  )(key: GlobalKeyWithMaintainers): Option[Value.ContractId] = {
    // we don't check whether the contract is active or not, because in we might not have loaded it earlier.
    // this is not a problem, because:
    // a) if the lookup was negative and we actually found a contract,
    //    the transaction validation will fail.
    // b) if the lookup was positive and its result is a different contract,
    //    the transaction validation will fail.
    // c) if the lookup was positive and its result is the same contract,
    //    - the authorization check ensures that the submitter is in fact allowed
    //      to lookup the contract
    //    - the separate contract keys check ensures that all contracts pointed to by
    //    contract keys respect causal monotonicity.
    val stateKey = Conversions.globalKeyToStateKey(key.globalKey)
    val contractId = for {
      stateValue <- commitContext.read(stateKey)
      if stateValue.getContractKeyState.getContractId.nonEmpty
    } yield decodeContractId(stateValue.getContractKeyState.getContractId)

    // If the key was not in state inputs, then we look whether any of the accessed contracts has
    // the key we're looking for. This happens with "fetchByKey" where the key lookup is not
    // evidenced in the transaction. The activeness of the contract is checked when it is fetched.
    contractId.orElse {
      knownKeys.get(stateKey.getContractKey)
    }
  }

  private def buildRejectionLogEntry(
      transactionEntry: DamlTransactionEntrySummary,
      reason: RejectionReason,
  ): DamlTransactionRejectionEntry.Builder = {
    logger.trace(
      s"Transaction rejected, ${reason.description}, correlationId=${transactionEntry.commandId}")
    val builder = DamlTransactionRejectionEntry.newBuilder
    builder
      .setSubmitterInfo(transactionEntry.submitterInfo)

    reason match {
      case RejectionReason.Inconsistent(reason) =>
        builder.setInconsistent(Inconsistent.newBuilder.setDetails(reason))
      case RejectionReason.Disputed(reason) =>
        builder.setDisputed(Disputed.newBuilder.setDetails(reason))
      case RejectionReason.ResourcesExhausted(reason) =>
        builder.setResourcesExhausted(ResourcesExhausted.newBuilder.setDetails(reason))
      case RejectionReason.PartyNotKnownOnLedger(reason) =>
        builder.setPartyNotKnownOnLedger(PartyNotKnownOnLedger.newBuilder.setDetails(reason))
      case RejectionReason.SubmitterCannotActViaParticipant(details) =>
        builder.setSubmitterCannotActViaParticipant(
          SubmitterCannotActViaParticipant.newBuilder
            .setDetails(details))
      case RejectionReason.InvalidLedgerTime(reason) =>
        builder.setInvalidLedgerTime(InvalidLedgerTime.newBuilder.setDetails(reason))
    }
    builder
  }

  private def reject[A](
      recordTime: Option[Timestamp],
      rejectionEntry: DamlTransactionRejectionEntry.Builder,
  ): StepResult[A] = {
    Metrics.rejections(rejectionEntry.getReasonCase.getNumber).inc()
    StepStop(
      buildLogEntryWithOptionalRecordTime(
        recordTime,
        _.setTransactionRejectionEntry(rejectionEntry)))
  }

  private object Metrics {
    val rejections: Map[Int, Counter] =
      DamlTransactionRejectionEntry.ReasonCase.values
        .map(v => v.getNumber -> metrics.daml.kvutils.committer.transaction.rejection(v.name()))
        .toMap
  }

}

private[kvutils] object TransactionCommitter {
  case class DamlTransactionEntrySummary(submission: DamlTransactionEntry) {
    val ledgerEffectiveTime: Timestamp = parseTimestamp(submission.getLedgerEffectiveTime)
    val submitterInfo: DamlSubmitterInfo = submission.getSubmitterInfo
    val commandId: String = submitterInfo.getCommandId
    val submitters: List[Party] =
      submitterInfo.getSubmittersList.asScala.toList.map(Party.assertFromString)
    lazy val transaction: Tx.Transaction = Conversions.decodeTransaction(submission.getTransaction)
    val submissionTime: Timestamp = Conversions.parseTimestamp(submission.getSubmissionTime)
    val submissionSeed: crypto.Hash = Conversions.parseHash(submission.getSubmissionSeed)
  }

  // Helper to read the _current_ contract state.
  // NOTE(JM): Important to fetch from the state that is currently being built up since
  // we mark some contracts as archived and may later change their disclosure and do not
  // want to "unarchive" them.
  def getContractState(commitContext: CommitContext, key: DamlStateKey): DamlContractState =
    commitContext.get(key).getOrElse(throw Err.MissingInputState(key)).getContractState

  @SuppressWarnings(Array("org.wartremover.warts.Option2Iterable"))
  private def transactionMinRecordTime(
      submissionTime: Instant,
      ledgerTime: Instant,
      maybeDeduplicateUntil: Option[Instant],
      timeModel: TimeModel): Instant =
    List(
      maybeDeduplicateUntil
        .map(_.plus(Timestamp.Resolution)), // DeduplicateUntil defines a rejection window, endpoints inclusive
      Some(timeModel.minRecordTime(ledgerTime)),
      Some(timeModel.minRecordTime(submissionTime))
    ).flatten.max

  private def transactionMaxRecordTime(
      submissionTime: Instant,
      ledgerTime: Instant,
      timeModel: TimeModel): Instant =
    List(timeModel.maxRecordTime(ledgerTime), timeModel.maxRecordTime(submissionTime)).min

  private def getLedgerDeduplicateUntil(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext): Option[Instant] =
    for {
      dedupEntry <- commitContext.get(commandDedupKey(transactionEntry.submitterInfo))
      dedupTimestamp <- PartialFunction.condOpt(dedupEntry.getCommandDedup.hasDeduplicatedUntil) {
        case true => dedupEntry.getCommandDedup.getDeduplicatedUntil
      }
    } yield parseTimestamp(dedupTimestamp).toInstant
}
