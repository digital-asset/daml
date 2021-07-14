// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions.{
  contractIdToStateKey,
  decodeContractId,
  packageStateKey,
  parseTimestamp,
}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractKey,
  DamlContractState,
  DamlStateValue,
}
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
import com.daml.lf.engine.{Engine, Error => LfError}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.{
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

private[transaction] class ModelConformanceValidator(engine: Engine, metrics: Metrics)
    extends TransactionValidator {
  import ModelConformanceValidator._

  private final val logger = ContextualizedLogger.get(getClass)

  /** Creates a committer step that validates the submission's conformance to the Daml model. */
  override def createValidationStep(rejections: Rejections): Step = new Step {
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
                rejections.buildRejectionStep(
                  transactionEntry,
                  rejectionReasonForValidationError(err),
                  commitContext.recordTime,
                ),
              _ => StepContinue[DamlTransactionEntrySummary](transactionEntry),
            )
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
      })
  }

  private def contractIsActive(
      transactionEntry: DamlTransactionEntrySummary,
      contractState: DamlContractState,
  ): Boolean = {
    val activeAt = Option(contractState.getActiveAt).map(parseTimestamp)
    !contractState.hasArchivedAt && activeAt.exists(transactionEntry.ledgerEffectiveTime >= _)
  }

  // Helper to lookup contract instances. We verify the activeness of
  // contract instances here. Since we look up every contract that was
  // an input to a transaction, we do not need to verify the inputs separately.
  private def lookupContract(
      transactionEntry: DamlTransactionEntrySummary,
      commitContext: CommitContext,
  )(
      coid: Value.ContractId
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
}

private[transaction] object ModelConformanceValidator {
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
