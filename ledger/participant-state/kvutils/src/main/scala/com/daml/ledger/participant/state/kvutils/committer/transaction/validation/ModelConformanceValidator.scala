// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions.{
  contractIdToStateKey,
  packageStateKey,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejection,
  Rejections,
  Step,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.{DamlContractState, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.lf.archive
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, Result}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Transaction.{
  DuplicateKeys,
  InconsistentKeys,
  KeyActive,
  KeyInput,
  KeyInputError,
}
import com.daml.lf.transaction.{GlobalKey, GlobalKeyWithMaintainers, SubmittedTransaction}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics

/** Validates the submission's conformance to the Daml model.
  *
  * @param engine An [[Engine]] instance to reinterpret and validate the transaction.
  * @param metrics A [[Metrics]] instance to record metrics.
  */
private[transaction] class ModelConformanceValidator(engine: Engine, metrics: Metrics)
    extends TransactionValidator {
  import ModelConformanceValidator._

  private final val logger = ContextualizedLogger.get(getClass)

  /** Validates model conformance based on the transaction itself (where it's possible).
    * Because fetch nodes don't contain contracts, we still need to get them from the current state ([[CommitContext]]).
    * It first reinterprets the transaction to detect a potentially malicious participant or bugs.
    * Then, checks the causal monotonicity.
    *
    * @param rejections A helper object for creating rejection [[Step]]s.
    * @return A committer [[Step]] that performs validation.
    */
  override def createValidationStep(rejections: Rejections): Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] =
      metrics.daml.kvutils.committer.transaction.interpretTimer.time(() => {
        val validationResult = engine.validate(
          transactionEntry.submitters.toSet,
          SubmittedTransaction(transactionEntry.transaction),
          transactionEntry.ledgerEffectiveTime,
          commitContext.participantId,
          transactionEntry.submissionTime,
          transactionEntry.submissionSeed,
        )

        for {
          stepResult <- consumeValidationResult(
            validationResult,
            transactionEntry,
            commitContext,
            rejections,
          )
          finalStepResult <- validateCausalMonotonicity(stepResult, commitContext, rejections)
        } yield finalStepResult
      })
  }

  private def consumeValidationResult(
      validationResult: Result[Unit],
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
      rejections: Rejections,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    try {
      val stepResult = for {
        contractKeyInputs <- transactionEntry.transaction.contractKeyInputs.left
          .map(rejectionForKeyInputError(transactionEntry, commitContext.recordTime, rejections))
        _ <- validationResult
          .consume(
            lookupContract(commitContext),
            lookupPackage(commitContext),
            lookupKey(contractKeyInputs),
          )
          .left
          .map(error =>
            rejections.reject(
              transactionEntry,
              Rejection.ValidationFailure(error),
              commitContext.recordTime,
            )
          )
      } yield ()
      stepResult.fold(identity, _ => StepContinue(transactionEntry))
    } catch {
      case missingInputErr @ Err.MissingInputState(key) =>
        logger.error(
          "Model conformance validation failed due to a missing input state (most likely due to invalid state on the participant).",
          missingInputErr,
        )
        rejections.reject(
          transactionEntry,
          Rejection.MissingInputState(key),
          commitContext.recordTime,
        )
      case err: Err =>
        logger.error(
          "Model conformance validation failed most likely due to invalid state on the participant.",
          err,
        )
        rejections.reject(
          transactionEntry,
          Rejection.InvalidParticipantState(err),
          commitContext.recordTime,
        )
    }
  }

  /** Helper to lookup contract instances. Since we look up every contract that was
    * an input to a transaction, we do not need to verify the inputs separately.
    *
    * Note that for an honest participant, a contract may not be in the state only if it was archived and pruned
    * on the committer. Then, an honest participant is able to produce such a transaction only by using
    * a divulged contract that appeared as active, because it didn't learn about the archival.
    * On the other hand, using divulged contracts for interpretation is deprecated so we turn it into [[Rejection.MissingInputState]].
    */
  @throws[Err.MissingInputState]
  private[validation] def lookupContract(
      commitContext: CommitContext
  )(
      contractId: Value.ContractId
  ): Option[Value.VersionedContractInstance] =
    commitContext
      .read(contractIdToStateKey(contractId))
      .map(_.getContractState)
      .map(_.getContractInstance)
      .map(Conversions.decodeContractInstance)

  /** Helper to lookup package from the state. The package contents are stored
    * in the [[com.daml.ledger.participant.state.kvutils.store.DamlLogEntry]],
    * which we find by looking up the Daml state entry at `DamlStateKey(packageId = pkgId)`.
    *
    * Note that there is no committer pruning of packages, so [[Rejection.MissingInputState]]
    * can only arise from a malicious or buggy participant.
    */
  @throws[Err.MissingInputState]
  @throws[Err.ArchiveDecodingFailed]
  private[validation] def lookupPackage(
      commitContext: CommitContext
  )(pkgId: PackageId)(implicit loggingContext: LoggingContext): Some[Ast.Package] =
    withEnrichedLoggingContext("packageId" -> pkgId) { implicit loggingContext =>
      val stateKey = packageStateKey(pkgId)
      val value = commitContext.read(stateKey).getOrElse {
        logger.warn("Package lookup failed, package not found.")
        throw Err.MissingInputState(stateKey)
      }
      value.getValueCase match {
        case DamlStateValue.ValueCase.ARCHIVE =>
          // NOTE(JM): Engine only looks up packages once, compiles and caches,
          // provided that the engine instance is persisted.
          archive.Decode.decodeArchive(value.getArchive) match {
            case Right((_, pkg)) => Some(pkg)
            case Left(err) =>
              logger.warn("Decoding the archive failed.")
              throw Err.ArchiveDecodingFailed(pkgId, err.getMessage)
          }

        case _ =>
          val msg = "value is not a Daml-LF archive"
          logger.warn(s"Package lookup failed, $msg.")
          throw Err.ArchiveDecodingFailed(pkgId, msg)
      }
    }

  private[validation] def lookupKey(
      contractKeyInputs: Map[GlobalKey, KeyInput]
  )(key: GlobalKeyWithMaintainers): Option[Value.ContractId] =
    contractKeyInputs.get(key.globalKey) match {
      case Some(KeyActive(cid)) => Some(cid)
      case _ => None
    }

  // Checks that input contracts have been created before or at the current ledger effective time.
  private[validation] def validateCausalMonotonicity(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
      rejections: Rejections,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {

    val inputContracts: Map[Value.ContractId, DamlContractState] = commitContext
      .collectInputs {
        case (key, Some(value)) if value.hasContractState =>
          Conversions.stateKeyToContractId(key) -> value.getContractState
      }

    val isCausallyMonotonic = transactionEntry.transaction.inputContracts.forall { contractId =>
      // Checking contract existence is part of contract consistency checks at a later validation step,
      // hence, we don't want to leak contract information to a potentially malicious participant here
      // by producing a rejection for non-existent contracts.
      // Some input contracts may no longer exist even if the participant is honest, as they may
      // have been archived and pruned by the committer.
      inputContracts.get(contractId).forall { damlContractState =>
        val activeAt = Option(damlContractState.getActiveAt).map(parseTimestamp)
        activeAt.exists(transactionEntry.ledgerEffectiveTime >= _)
      }
    }

    if (isCausallyMonotonic)
      StepContinue(transactionEntry)
    else
      rejections.reject(
        transactionEntry,
        Rejection.CausalMonotonicityViolated,
        commitContext.recordTime,
      )
  }
}

private[transaction] object ModelConformanceValidator {

  private def rejectionForKeyInputError(
      transactionEntry: DamlTransactionEntrySummary,
      recordTime: Option[Timestamp],
      rejections: Rejections,
  )(
      error: KeyInputError
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    val rejection = error match {
      case DuplicateKeys(_) =>
        Rejection.InternallyInconsistentTransaction.DuplicateKeys
      case InconsistentKeys(_) =>
        Rejection.InternallyInconsistentTransaction.InconsistentKeys
    }
    rejections.reject(transactionEntry, rejection, recordTime)
  }
}
