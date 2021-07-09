// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.committer._
import com.daml.ledger.participant.state.kvutils.committer.transaction.validation.{
  ContractKeysValidator,
  LedgerTimeValidator,
  ModelConformanceValidator,
}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.{Blinding, Engine}
import com.daml.lf.transaction.{BlindingInfo, TransactionOuterClass}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.google.protobuf.{Timestamp => ProtoTimestamp}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

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
    inStaticTimeMode: Boolean,
) extends Committer[DamlTransactionEntrySummary] {

  import TransactionCommitter._

  private final val logger = ContextualizedLogger.get(getClass)

  override protected val committerName = "transaction"

  override protected def extraLoggingContext(
      transactionEntry: DamlTransactionEntrySummary
  ): LoggingEntries =
    LoggingEntries(
      "submitters" -> transactionEntry.submitters
    )

  override protected def init(
      commitContext: CommitContext,
      submission: DamlSubmission,
  )(implicit loggingContext: LoggingContext): DamlTransactionEntrySummary =
    DamlTransactionEntrySummary(submission.getTransactionEntry)

  private val rejections = new Rejections(metrics)
  private val ledgerTimeValidator = new LedgerTimeValidator(defaultConfig)
  private val modelConformanceValidator = new ModelConformanceValidator(engine, metrics)

  override protected val steps: Steps[DamlTransactionEntrySummary] = Iterable(
    "authorize_submitter" -> authorizeSubmitters,
    "check_informee_parties_allocation" -> checkInformeePartiesAllocation,
    "deduplicate" -> deduplicateCommand,
    "validate_ledger_time" -> ledgerTimeValidator.createValidationStep(rejections),
    "validate_contract_keys" -> ContractKeysValidator.createValidationStep(rejections),
    "validate_model_conformance" -> modelConformanceValidator.createValidationStep(
      rejections
    ),
    "blind" -> blind,
    "trim_unnecessary_nodes" -> trimUnnecessaryNodes,
    "build_final_log_entry" -> buildFinalLogEntry,
  )

  /** Reject duplicate commands
    */
  private[transaction] def deduplicateCommand: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      commitContext.recordTime
        .map { recordTime =>
          val dedupKey = commandDedupKey(transactionEntry.submitterInfo)
          val dedupEntry = commitContext.get(dedupKey)
          val submissionTime =
            if (inStaticTimeMode) Instant.now() else recordTime.toInstant
          if (dedupEntry.forall(isAfterDeduplicationTime(submissionTime, _))) {
            StepContinue(transactionEntry)
          } else {
            logger.trace("Transaction rejected because the command is a duplicate.")
            rejections.buildRejectionStep(
              DamlTransactionRejectionEntry.newBuilder
                .setSubmitterInfo(transactionEntry.submitterInfo)
                .setDuplicateCommand(Duplicate.newBuilder.setDetails("")),
              commitContext.recordTime,
            )
          }
        }
        .getOrElse(StepContinue(transactionEntry))
    }
  }

  // Checks that the submission time of the command is after the
  // deduplicationTime represented by stateValue
  private def isAfterDeduplicationTime(
      submissionTime: Instant,
      stateValue: DamlStateValue,
  ): Boolean = {
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
  private[transaction] def authorizeSubmitters: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
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
                RejectionReasonV0.SubmitterCannotActViaParticipant(
                  s"Party '$submitter' not hosted by participant ${commitContext.participantId}"
                )
              )
            )
          case None =>
            Some(
              rejection(
                RejectionReasonV0.PartyNotKnownOnLedger(s"Submitting party '$submitter' not known")
              )
            )
        }

      def rejection(reason: RejectionReasonV0): StepResult[DamlTransactionEntrySummary] =
        rejections.buildRejectionStep(
          transactionEntry,
          reason,
          commitContext.recordTime,
        )

      authorizeAll(transactionEntry.submitters)
    }
  }

  /** Validate ledger effective time and the command's time-to-live. */
  private[transaction] def validateLedgerTime: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
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
                    RejectionReasonV0.InvalidLedgerTime(reason),
                  ),
                ),
              _ => StepContinue(transactionEntry),
            )
        case None => // Pre-execution: propagate the time bounds and defer the checks to post-execution.
          val maybeDeduplicateUntil =
            getLedgerDeduplicateUntil(transactionEntry, commitContext)
          val minimumRecordTime = transactionMinRecordTime(
            transactionEntry.submissionTime.toInstant,
            transactionEntry.ledgerEffectiveTime.toInstant,
            maybeDeduplicateUntil,
            timeModel,
          )
          val maximumRecordTime = transactionMaxRecordTime(
            transactionEntry.submissionTime.toInstant,
            transactionEntry.ledgerEffectiveTime.toInstant,
            timeModel,
          )
          commitContext.deduplicateUntil = maybeDeduplicateUntil
          commitContext.minimumRecordTime = Some(minimumRecordTime)
          commitContext.maximumRecordTime = Some(maximumRecordTime)
          val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
            .setTransactionRejectionEntry(
              buildRejectionLogEntry(
                transactionEntry,
                RejectionReasonV0.InvalidLedgerTime(
                  s"Record time is outside of valid range [$minimumRecordTime, $maximumRecordTime]"
                ),
              )
            )
            .build
          commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
          StepContinue(transactionEntry)
      }
    }
  }

  /** Validate the submission's conformance to the Daml model */
  private def validateModelConformance: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] =
      metrics.daml.kvutils.committer.transaction.interpretTimer.time(() => {
        // Pull all keys from referenced contracts. We require this for 'fetchByKey' calls
        // which are not evidenced in the transaction itself and hence the contract key state is
        // not included in the inputs.
        lazy val knownKeys: Map[DamlContractKey, Value.ContractId] =
          commitContext.collectInputs {
            case (key, Some(value))
                if value.getContractState.hasContractKey
                  && contractIsActive(transactionEntry, value.getContractState) =>
              value.getContractState.getContractKey -> Conversions
                .stateKeyToContractId(key)
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
              lookupPackage(commitContext),
              lookupKey(commitContext, knownKeys),
            )
            .fold(
              err =>
                reject[DamlTransactionEntrySummary](
                  commitContext.recordTime,
                  buildRejectionLogEntry(transactionEntry, rejectionReasonForValidationError(err)),
                ),
              _ => StepContinue[DamlTransactionEntrySummary](transactionEntry),
            )
        } catch {
          case err: Err.MissingInputState =>
            logger.warn(
              "Model conformance validation failed due to a missing input state (most likely due to invalid state on the participant)."
            )
            reject(
              commitContext.recordTime,
              buildRejectionLogEntry(transactionEntry, RejectionReasonV0.Disputed(err.getMessage)),
            )
        }
      })
  }

  private[transaction] def rejectionReasonForValidationError(
      validationError: LfError
  ): RejectionReasonV0 = {
    def disputed: RejectionReasonV0 =
      RejectionReasonV0.Disputed(validationError.msg)

    def resultIsCreatedInTx(
        tx: VersionedTransaction[NodeId, ContractId],
        result: Option[Value.ContractId],
    ): Boolean =
      result.exists { contractId =>
        tx.nodes.exists {
          case (_, create: Node.NodeCreate[_]) => create.coid == contractId
          case _ => false
        }
      }

    validationError match {
      case LfError.Validation(
            LfError.Validation.ReplayMismatch(
              ReplayNodeMismatch(recordedTx, recordedNodeId, replayedTx, replayedNodeId)
            )
          ) =>
        // If the problem is that a key lookup has changed and the results do not involve contracts created in this transaction,
        // then it's a consistency problem.

        (recordedTx.nodes(recordedNodeId), replayedTx.nodes(replayedNodeId)) match {
          case (
                Node.NodeLookupByKey(
                  recordedTemplateId,
                  recordedKey,
                  recordedResult,
                  recordedVersion,
                ),
                Node.NodeLookupByKey(
                  replayedTemplateId,
                  replayedKey,
                  replayedResult,
                  replayedVersion,
                ),
              )
              if recordedVersion == replayedVersion &&
                recordedTemplateId == replayedTemplateId && recordedKey == replayedKey
                && !resultIsCreatedInTx(recordedTx, recordedResult)
                && !resultIsCreatedInTx(replayedTx, replayedResult) =>
            RejectionReasonV0.Inconsistent(validationError.msg)
          case _ => disputed
        }
      case _ => disputed
    }
  }

  /** Validate the submission's conformance to the Daml model */
  private[transaction] def blind: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val blindingInfo = Blinding.blind(transactionEntry.transaction)
      setDedupEntryAndUpdateContractState(
        commitContext,
        transactionEntry.copyPreservingDecodedTransaction(
          submission = transactionEntry.submission.toBuilder
            .setBlindingInfo(Conversions.encodeBlindingInfo(blindingInfo))
            .build
        ),
        blindingInfo,
      )
    }
  }

  /** Removes `Fetch` and `LookupByKey` nodes from the transactionEntry.
    */
  private[transaction] def trimUnnecessaryNodes: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val transaction = transactionEntry.submission.getTransaction
      val nodes = transaction.getNodesList.asScala
      val nodeMap: Map[String, TransactionOuterClass.Node] =
        nodes.view.map(n => n.getNodeId -> n).toMap

      @tailrec
      def goNodesToKeep(todo: List[String], result: Set[String]): Set[String] = todo match {
        case Nil => result
        case head :: tail =>
          import TransactionOuterClass.Node.NodeTypeCase
          val node = nodeMap
            .get(head)
            .getOrElse(throw Err.InternalError(s"Invalid transaction node id $head"))
          node.getNodeTypeCase match {
            case NodeTypeCase.CREATE =>
              goNodesToKeep(tail, result + head)
            case NodeTypeCase.EXERCISE =>
              goNodesToKeep(node.getExercise.getChildrenList.asScala.toList ++ tail, result + head)
            case NodeTypeCase.ROLLBACK | NodeTypeCase.FETCH | NodeTypeCase.LOOKUP_BY_KEY |
                NodeTypeCase.NODETYPE_NOT_SET =>
              goNodesToKeep(tail, result)
          }
      }

      val nodesToKeep = goNodesToKeep(transaction.getRootsList.asScala.toList, Set.empty)

      val filteredRoots = transaction.getRootsList.asScala.filter(nodesToKeep)

      def stripUnnecessaryNodes(node: TransactionOuterClass.Node): TransactionOuterClass.Node =
        if (node.hasExercise) {
          val exerciseNode = node.getExercise
          val keptChildren =
            exerciseNode.getChildrenList.asScala.filter(nodesToKeep)
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
  }

  /** Builds the log entry as the final step.
    */
  private def buildFinalLogEntry: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = StepStop(
      buildLogEntry(transactionEntry, commitContext)
    )
  }

  /** Check that all informee parties mentioned of a transaction are allocated. */
  private def checkInformeePartiesAllocation: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val parties = transactionEntry.transaction.informees
      if (parties.forall(party => commitContext.get(partyStateKey(party)).isDefined))
        StepContinue(transactionEntry)
      else
        rejections.buildRejectionStep(
          transactionEntry,
          RejectionReasonV0.PartyNotKnownOnLedger("Not all parties known"),
          commitContext.recordTime,
        )
    }
  }

  /** Produce the log entry and contract state updates. */
  private def setDedupEntryAndUpdateContractState(
      commitContext: CommitContext,
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    // Set a deduplication entry.
    commitContext.set(
      commandDedupKey(transactionEntry.submitterInfo),
      DamlStateValue.newBuilder
        .setCommandDedup(
          DamlCommandDedupValue.newBuilder
            .setDeduplicatedUntil(transactionEntry.submitterInfo.getDeduplicateUntil)
            .build
        )
        .build,
    )

    updateContractState(transactionEntry, blindingInfo, commitContext)

    metrics.daml.kvutils.committer.transaction.accepts.inc()
    logger.trace("Transaction accepted.")
    StepContinue(transactionEntry)
  }

  private def updateContractState(
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo,
      commitContext: CommitContext,
  )(implicit loggingContext: LoggingContext): Unit = {
    val localContracts = transactionEntry.transaction.localContracts
    val consumedContracts = transactionEntry.transaction.consumedContracts
    val contractKeys = transactionEntry.transaction.updatedContractKeys
    // Add contract state entries to mark contract activeness (checked by 'validateModelConformance').
    for ((cid, (nid, createNode)) <- localContracts) {
      val cs = DamlContractState.newBuilder
      cs.setActiveAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
      val localDisclosure = blindingInfo.disclosure(nid)
      cs.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
      cs.setContractInstance(
        Conversions.encodeContractInstance(createNode.versionedCoinst)
      )
      createNode.key.foreach { keyWithMaintainers =>
        cs.setContractKey(
          Conversions.encodeContractKey(createNode.coinst.template, keyWithMaintainers.key)
        )
      }
      commitContext.set(
        Conversions.contractIdToStateKey(cid),
        DamlStateValue.newBuilder.setContractState(cs).build,
      )
    }
    // Update contract state entries to mark contracts as consumed (checked by 'validateModelConformance').
    consumedContracts.foreach { cid =>
      val key = Conversions.contractIdToStateKey(cid)
      val cs = getContractState(commitContext, key)
      commitContext.set(
        key,
        DamlStateValue.newBuilder
          .setContractState(
            cs.toBuilder
              .setArchivedAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
          )
          .build,
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
    for ((contractKey, contractKeyState) <- contractKeys) {
      val stateKey = Conversions.globalKeyToStateKey(contractKey)
      val (k, v) =
        updateContractKeyWithContractKeyState(ledgerEffectiveTime, stateKey, contractKeyState)
      commitContext.set(k, v)
    }
  }

  private def updateContractKeyWithContractKeyState(
      ledgerEffectiveTime: ProtoTimestamp,
      key: DamlStateKey,
      contractKeyState: Option[ContractId],
  )(implicit loggingContext: LoggingContext): (DamlStateKey, DamlStateValue) = {
    logger.trace(s"Updating contract key $key to $contractKeyState.")
    key ->
      DamlStateValue.newBuilder
        .setContractKeyState(
          contractKeyState
            .map(coid =>
              DamlContractKeyState.newBuilder
                .setContractId(coid.coid)
                .setActiveAt(ledgerEffectiveTime)
            )
            .getOrElse(DamlContractKeyState.newBuilder())
        )
        .build
  }
}

private[kvutils] object TransactionCommitter {

  def buildLogEntry(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
  ): DamlLogEntry = {
    if (commitContext.preExecute) {
      val outOfTimeBoundsLogEntry = DamlLogEntry.newBuilder
        .setTransactionRejectionEntry(
          DamlTransactionRejectionEntry.newBuilder
            .setSubmitterInfo(transactionEntry.submitterInfo)
        )
        .build
      commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
    }
    buildLogEntryWithOptionalRecordTime(
      commitContext.recordTime,
      _.setTransactionEntry(transactionEntry.submission),
    )
  }

  // Helper to read the _current_ contract state.
  // NOTE(JM): Important to fetch from the state that is currently being built up since
  // we mark some contracts as archived and may later change their disclosure and do not
  // want to "unarchive" them.
  def getContractState(commitContext: CommitContext, key: DamlStateKey): DamlContractState =
    commitContext
      .get(key)
      .getOrElse(throw Err.MissingInputState(key))
      .getContractState
}
