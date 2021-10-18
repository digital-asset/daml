// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import java.time.Instant
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.committer._
import com.daml.ledger.participant.state.kvutils.committer.transaction.validation.{
  LedgerTimeValidator,
  ModelConformanceValidator,
  TransactionConsistencyValidator,
}
import com.daml.ledger.participant.state.kvutils.store.events.{
  DamlTransactionRejectionEntry,
  Duplicate,
}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlCommandDedupValue,
  DamlContractKeyState,
  DamlContractState,
  DamlLogEntry,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.lf.data.Ref.Party
import com.daml.lf.engine.{Blinding, Engine}
import com.daml.lf.kv.TransactionConverter
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
    "overwrite_deduplication_period" -> overwriteDeduplicationPeriodWithMaxDuration,
    "deduplicate" -> deduplicateCommand,
    "validate_ledger_time" -> ledgerTimeValidator.createValidationStep(rejections),
    "validate_model_conformance" -> modelConformanceValidator.createValidationStep(rejections),
    "validate_consistency" -> TransactionConsistencyValidator.createValidationStep(rejections),
    "blind" -> blind,
    "trim_unnecessary_nodes" -> trimUnnecessaryNodes,
    "build_final_log_entry" -> buildFinalLogEntry,
  )

  private[transaction] def overwriteDeduplicationPeriodWithMaxDuration: Step = new Step {
    override def apply(context: CommitContext, input: DamlTransactionEntrySummary)(implicit
        loggingContext: LoggingContext
    ): StepResult[DamlTransactionEntrySummary] = {
      val (_, currentConfig) = getCurrentConfiguration(defaultConfig, context)
      val submission = input.submission.toBuilder
      submission.getSubmitterInfoBuilder.setDeduplicationDuration(
        buildDuration(currentConfig.maxDeduplicationTime)
      )
      StepContinue(input.copyPreservingDecodedTransaction(submission.build()))
    }
  }

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
            rejections.reject(
              DamlTransactionRejectionEntry.newBuilder
                .setSubmitterInfo(transactionEntry.submitterInfo)
                // No duplicate rejection is a definite answer as the deduplication entry will eventually expire.
                .setDefiniteAnswer(false)
                .setDuplicateCommand(Duplicate.newBuilder.setDetails("")),
              "the command is a duplicate",
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
              reject(
                Rejection.SubmitterCannotActViaParticipant(submitter, commitContext.participantId)
              )
            )
          case None =>
            Some(reject(Rejection.SubmittingPartyNotKnownOnLedger(submitter)))
        }

      def reject(reason: Rejection): StepResult[DamlTransactionEntrySummary] =
        rejections.reject(
          transactionEntry,
          reason,
          commitContext.recordTime,
        )

      authorizeAll(transactionEntry.submitters)
    }
  }

  /** Set blinding info. */
  private[transaction] def blind: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      setDedupEntry(commitContext, transactionEntry)

      val blindingInfo = Blinding.blind(transactionEntry.transaction)

      val divulgedContracts =
        updateContractStateAndFetchDivulgedContracts(transactionEntry, blindingInfo, commitContext)

      metrics.daml.kvutils.committer.transaction.accepts.inc()
      logger.trace("Transaction accepted.")

      val transactionEntryWithBlindingInfo =
        transactionEntry.copyPreservingDecodedTransaction(
          submission = transactionEntry.submission.toBuilder
            .setBlindingInfo(Conversions.encodeBlindingInfo(blindingInfo, divulgedContracts))
            .build
        )

      StepContinue(transactionEntryWithBlindingInfo)
    }
  }

  /** Removes `Fetch` and `LookupByKey` nodes from the transactionEntry.
    */
  private[transaction] def trimUnnecessaryNodes: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val rawTransaction = transactionEntry.submission.getTransaction
      val transaction = rawTransaction.unpack(classOf[TransactionOuterClass.Transaction])
      val nodes = transaction.getNodesList.asScala
      val nodeMap: Map[String, TransactionOuterClass.Node] =
        nodes.view.map(n => n.getNodeId -> n).toMap

      @tailrec
      def goNodesToKeep(todo: List[String], result: Set[String]): Set[String] = todo match {
        case Nil => result
        case head :: tail =>
          import TransactionOuterClass.Node.NodeTypeCase
          val node =
            nodeMap.getOrElse(head, throw Err.InternalError(s"Invalid transaction node id $head"))
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
        .setTransaction(com.google.protobuf.Any.pack(newTransaction))
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
      val missingParties = parties.filter(party => commitContext.get(partyStateKey(party)).isEmpty)
      if (missingParties.isEmpty)
        StepContinue(transactionEntry)
      else
        rejections.reject(
          transactionEntry,
          Rejection.PartiesNotKnownOnLedger(missingParties),
          commitContext.recordTime,
        )
    }
  }

  private[transaction] def setDedupEntry(
      commitContext: CommitContext,
      transactionEntry: DamlTransactionEntrySummary,
  )(implicit loggingContext: LoggingContext): Unit = {
    val (_, config) = getCurrentConfiguration(defaultConfig, commitContext)
    // Deduplication duration must be explicitly overwritten in a previous step
    //  (see [[TransactionCommitter.overwriteDeduplicationPeriodWithMaxDuration]]) and set to ``config.maxDeduplicationTime``.
    if (!transactionEntry.submitterInfo.hasDeduplicationDuration) {
      throw Err.InvalidSubmission("Deduplication duration is not set.")
    }
    val commandDedupBuilder = DamlCommandDedupValue.newBuilder.setDeduplicatedUntil(
      Conversions.buildTimestamp(
        transactionEntry.submissionTime
          .add(parseDuration(transactionEntry.submitterInfo.getDeduplicationDuration))
          .add(config.timeModel.minSkew)
      )
    )
    // Set a deduplication entry.
    commitContext.set(
      commandDedupKey(transactionEntry.submitterInfo),
      DamlStateValue.newBuilder
        .setCommandDedup(
          commandDedupBuilder.build
        )
        .build,
    )
  }

  private def updateContractStateAndFetchDivulgedContracts(
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo,
      commitContext: CommitContext,
  )(implicit
      loggingContext: LoggingContext
  ): Map[ContractId, com.google.protobuf.Any] = {
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
          Conversions.encodeContractKey(createNode.templateId, keyWithMaintainers.key)
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
    val divulgedContractsBuilder = {
      val builder = Map.newBuilder[ContractId, com.google.protobuf.Any]
      builder.sizeHint(blindingInfo.divulgence.size)
      builder
    }

    for ((coid, parties) <- blindingInfo.divulgence) {
      val key = contractIdToStateKey(coid)
      val cs = getContractState(commitContext, key)
      divulgedContractsBuilder += (coid -> com.google.protobuf.Any.pack(cs.getContractInstance))
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

    divulgedContractsBuilder.result()
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
            .setDefiniteAnswer(false)
            .setSubmitterInfo(transactionEntry.submitterInfo)
        )
        .build
      commitContext.outOfTimeBoundsLogEntry = Some(outOfTimeBoundsLogEntry)
    }
    // FIXME
    val transaction = transactionEntry.submission.getTransaction
      .unpack(classOf[TransactionOuterClass.Transaction])
    buildLogEntryWithOptionalRecordTime(
      commitContext.recordTime,
      _.setTransactionEntry(
        transactionEntry.submission.toBuilder
          .setTransactionVersion(transaction.getVersion)
          .putAllNodeIdToTransactionNode(
            transaction.getNodesList.asScala
              .map(node => node.getNodeId -> com.google.protobuf.Any.pack(node))
              .toMap
              .asJava
          )
          .addAllWitnessingParties(
            TransactionConverter
              .transactionToWitnesses(
                transaction
              )
              .asJava
          )
      ),
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
