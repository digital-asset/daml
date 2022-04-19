// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Copyright
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction

import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.Conversions._
import com.daml.ledger.participant.state.kvutils.committer.Committer._
import com.daml.ledger.participant.state.kvutils.committer._
import com.daml.ledger.participant.state.kvutils.committer.transaction.validation.{
  CommitterModelConformanceValidator,
  LedgerTimeValidator,
  TransactionConsistencyValidator,
}
import com.daml.ledger.participant.state.kvutils.store.events.DamlTransactionRejectionEntry
import com.daml.ledger.participant.state.kvutils.store.{
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
import com.daml.lf.kv.ConversionError
import com.daml.lf.kv.contracts.{ContractConversions, RawContractInstance}
import com.daml.lf.kv.transactions.{RawTransaction, TransactionConversions}
import com.daml.lf.transaction.BlindingInfo
import com.daml.lf.value.Value.ContractId
import com.daml.logging.entries.LoggingEntries
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.google.protobuf.{Timestamp => ProtoTimestamp}

import scala.jdk.CollectionConverters._

private[kvutils] class TransactionCommitter(
    defaultConfig: Configuration,
    engine: Engine,
    override protected val metrics: Metrics,
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
  private val committerModelConformanceValidator =
    new CommitterModelConformanceValidator(engine, metrics)

  /** The order of the steps matters
    */
  override protected val steps: Steps[DamlTransactionEntrySummary] = Iterable(
    "set_context_min_max_record_time" -> TimeBoundBindingStep.setTimeBoundsInContextStep(
      defaultConfig
    ),
    "set_context_out_of_time_bounds_entry" -> ledgerTimeValidator.createValidationStep(rejections),
    "authorize_submitter" -> authorizeSubmitters,
    "check_informee_parties_allocation" -> checkInformeePartiesAllocation,
    "deduplicate" -> CommandDeduplication.deduplicateCommandStep(rejections),
    "validate_committer_model_conformance" -> committerModelConformanceValidator
      .createValidationStep(rejections),
    "validate_consistency" -> TransactionConsistencyValidator.createValidationStep(rejections),
    "set_deduplication_entry" -> CommandDeduplication.setDeduplicationEntryStep(defaultConfig),
    "blind" -> blind,
    "trim_unnecessary_nodes" -> trimUnnecessaryNodes,
    "build_final_log_entry" -> buildFinalLogEntry,
  )

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

  /** Removes `Fetch`, `LookupByKey` and `Rollback` nodes from the transactionEntry.
    */
  private[transaction] def trimUnnecessaryNodes: Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val rawTransaction = RawTransaction(transactionEntry.submission.getRawTransaction)
      val newRawTransaction = TransactionConversions
        .keepCreateAndExerciseNodes(rawTransaction)
        .fold(
          {
            case ConversionError.InternalError(errorMessage) =>
              throw Err.InternalError(errorMessage)
            case error => throw Err.DecodeError("Transaction", error.errorMessage)
          },
          identity,
        )

      val newTransactionEntry = transactionEntry.submission.toBuilder
        .setRawTransaction(newRawTransaction.byteString)
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

  private def updateContractStateAndFetchDivulgedContracts(
      transactionEntry: DamlTransactionEntrySummary,
      blindingInfo: BlindingInfo,
      commitContext: CommitContext,
  )(implicit
      loggingContext: LoggingContext
  ): Map[ContractId, RawContractInstance] = {
    val localContracts = transactionEntry.transaction.localContracts
    val consumedContracts = transactionEntry.transaction.consumedContracts
    val contractKeys = transactionEntry.transaction.updatedContractKeys
    // Add contract state entries to mark contract activeness (checked by 'validateModelConformance').
    for ((cid, (nid, createNode)) <- localContracts) {
      val contractStateBuilder = DamlContractState.newBuilder
      val localDisclosure = blindingInfo.disclosure(nid)
      val rawContractInstance = ContractConversions
        .encodeContractInstance(createNode.versionedCoinst)
        .fold(err => throw Err.EncodeError("ContractInstance", err.errorMessage), identity)

      contractStateBuilder.setActiveAt(buildTimestamp(transactionEntry.ledgerEffectiveTime))
      contractStateBuilder.addAllLocallyDisclosedTo((localDisclosure: Iterable[String]).asJava)
      contractStateBuilder.setRawContractInstance(
        rawContractInstance.byteString
      )
      createNode.key.foreach { keyWithMaintainers =>
        contractStateBuilder.setContractKey(
          Conversions.encodeContractKey(createNode.templateId, keyWithMaintainers.key)
        )
      }

      commitContext.set(
        Conversions.contractIdToStateKey(cid),
        DamlStateValue.newBuilder.setContractState(contractStateBuilder).build,
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
      val builder = Map.newBuilder[ContractId, RawContractInstance]
      builder.sizeHint(blindingInfo.divulgence.size)
      builder
    }

    for ((coid, parties) <- blindingInfo.divulgence) {
      val key = contractIdToStateKey(coid)
      val contractState = getContractState(commitContext, key)
      divulgedContractsBuilder += (coid -> RawContractInstance(
        contractState.getRawContractInstance
      ))
      val divulged: Set[String] = contractState.getDivulgedToList.asScala.toSet
      val newDivulgences: Set[String] = parties.toSet[String] -- divulged
      if (newDivulgences.nonEmpty) {
        val newContractState = contractState.toBuilder
          .addAllDivulgedTo(newDivulgences.asJava)
        commitContext.set(key, DamlStateValue.newBuilder.setContractState(newContractState).build)
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
