// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions.{
  contractIdToStateKey,
  packageStateKey,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlContractState, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejections,
  Step,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.{Conversions, Err}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.archive
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.engine.{Engine, Result, Error => LfError}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Transaction.{
  DuplicateKeys,
  InconsistentKeys,
  KeyActive,
  KeyInput,
  KeyInputError,
}
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  ReplayNodeMismatch,
  SubmittedTransaction,
  VersionedTransaction,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
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
            rejections.buildRejectionStep(
              transactionEntry,
              rejectionReasonForValidationError(error),
              commitContext.recordTime,
            )
          )
      } yield ()
      stepResult.fold(identity, _ => StepContinue(transactionEntry))
    } catch {
      case err: Err.MissingInputState =>
        logger.warn(
          "Model conformance validation failed due to a missing input state (most likely due to invalid state on the participant)."
        )
        rejections.buildRejectionStep(
          transactionEntry,
          RejectionReasonV0.Disputed(err.getMessage),
          commitContext.recordTime,
        )
    }
  }

  // Helper to lookup contract instances. Since we look up every contract that was
  // an input to a transaction, we do not need to verify the inputs separately.
  private[validation] def lookupContract(
      commitContext: CommitContext
  )(
      contractId: Value.ContractId
  ): Option[Value.ContractInst[Value.VersionedValue[Value.ContractId]]] =
    commitContext
      .read(contractIdToStateKey(contractId))
      .map(_.getContractState)
      .map(_.getContractInstance)
      .map(Conversions.decodeContractInstance)

  // Helper to lookup package from the state. The package contents
  // are stored in the [[DamlLogEntry]], which we find by looking up
  // the Daml state entry at `DamlStateKey(packageId = pkgId)`.
  private def lookupPackage(
      commitContext: CommitContext
  )(pkgId: PackageId)(implicit loggingContext: LoggingContext): Option[Ast.Package] =
    withEnrichedLoggingContext("packageId" -> pkgId) { implicit loggingContext =>
      val stateKey = packageStateKey(pkgId)
      for {
        value <- commitContext
          .read(stateKey)
          .orElse {
            logger.warn("Package lookup failed, package not found.")
            throw Err.MissingInputState(stateKey)
          }
        pkg <- value.getValueCase match {
          case DamlStateValue.ValueCase.ARCHIVE =>
            // NOTE(JM): Engine only looks up packages once, compiles and caches,
            // provided that the engine instance is persisted.
            try {
              Some(archive.Decode.decode(value.getArchive)._2)
            } catch {
              case err: archive.Error =>
                logger.warn("Decoding the archive failed.")
                throw Err.DecodeError("Archive", err.getMessage)
            }

          case _ =>
            val msg = "value is not a Daml-LF archive"
            logger.warn(s"Package lookup failed, $msg.")
            throw Err.DecodeError("Archive", msg)
        }
      } yield pkg
    }

  private[validation] def lookupKey(
      contractKeyInputs: Map[GlobalKey, KeyInput]
  )(key: GlobalKeyWithMaintainers): Option[Value.ContractId] = {
    contractKeyInputs.get(key.globalKey) match {
      case Some(KeyActive(cid)) => Some(cid)
      case _ => None
    }
  }

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

    val isCasualMonotonicityHeld = transactionEntry.transaction.inputContracts.forall {
      contractId =>
        val inputContractState = inputContracts(contractId)
        val activeAt = Option(inputContractState.getActiveAt).map(parseTimestamp)
        activeAt.exists(transactionEntry.ledgerEffectiveTime >= _)
    }

    if (isCasualMonotonicityHeld)
      StepContinue(transactionEntry)
    else
      rejections.buildRejectionStep(
        transactionEntry,
        RejectionReasonV0.InvalidLedgerTime("Causal monotonicity violated"),
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
    val description = error match {
      case DuplicateKeys(_) =>
        "DuplicateKeys: the transaction contains a duplicate key"
      case InconsistentKeys(_) =>
        "InconsistentKeys: the transaction is internally inconsistent"
    }
    rejections.buildRejectionStep(
      transactionEntry,
      RejectionReasonV0.Disputed(description),
      recordTime,
    )
  }

  def rejectionReasonForValidationError(
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
                  _,
                  recordedKey,
                  recordedResult,
                  recordedVersion,
                ),
                Node.NodeLookupByKey(
                  replayedTemplateId,
                  _,
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
}
