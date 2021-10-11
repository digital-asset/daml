// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Rejection,
  Rejections,
  Step,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.kvutils.store.{
  DamlContractKey,
  DamlContractKeyState,
  DamlContractState,
  DamlStateKey,
}
import com.daml.lf.transaction.Transaction.{
  DuplicateKeys,
  InconsistentKeys,
  KeyActive,
  KeyCreate,
  NegativeKeyLookup,
}
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext

private[transaction] object TransactionConsistencyValidator extends TransactionValidator {

  /** Validates consistency of contracts and contract keys against the current ledger state.
    * For contracts, checks whether all contracts used in the transaction are still active.
    * For keys, checks whether they are consistent and there are no duplicates.
    *
    * @param rejections A helper object for creating rejection [[Step]]s.
    * @return A committer [[Step]] that performs validation.
    */
  override def createValidationStep(rejections: Rejections): Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      for {
        stepResult <- validateConsistencyOfKeys(
          commitContext,
          transactionEntry,
          rejections,
        )
        finalStepResult <- validateConsistencyOfContracts(
          commitContext,
          stepResult,
          rejections,
        )
      } yield finalStepResult
    }
  }

  private def validateConsistencyOfKeys(
      commitContext: CommitContext,
      transactionEntry: DamlTransactionEntrySummary,
      rejections: Rejections,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {

    val contractKeyState: Map[DamlStateKey, DamlContractKeyState] = commitContext.collectInputs {
      case (key, Some(value)) if key.hasContractKey => key -> value.getContractKeyState
    }
    val contractKeysToContractIds: Map[DamlContractKey, RawContractId] = contractKeyState.collect {
      case (k, v) if v.getContractId.nonEmpty =>
        k.getContractKey -> v.getContractId
    }

    val transaction = transactionEntry.transaction

    import scalaz.std.either._
    import scalaz.std.list._
    import scalaz.syntax.foldable._
    val keysValidationOutcome = for {
      keyInputs <- transaction.contractKeyInputs.left.map {
        case DuplicateKeys(_) => Duplicate
        case InconsistentKeys(_) => Inconsistent
      }
      _ <- keyInputs.toList.traverse_ { case (key, keyInput) =>
        val submittedDamlContractKey = Conversions.encodeGlobalKey(key)
        (contractKeysToContractIds.get(submittedDamlContractKey), keyInput) match {
          case (Some(_), KeyCreate) => Left(Duplicate)
          case (Some(_), NegativeKeyLookup) => Left(Inconsistent)
          case (Some(cid), KeyActive(submitted)) =>
            if (cid != submitted.coid)
              Left(Inconsistent)
            else
              Right(())
          case (None, KeyActive(_)) => Left(Inconsistent)
          case (None, KeyCreate | NegativeKeyLookup) => Right(())
        }
      }
    } yield ()

    keysValidationOutcome match {
      case Right(_) =>
        StepContinue(transactionEntry)
      case Left(error) =>
        val rejection = error match {
          case Duplicate =>
            Rejection.ExternallyInconsistentTransaction.DuplicateKeys
          case Inconsistent =>
            Rejection.ExternallyInconsistentTransaction.InconsistentKeys
        }
        rejections.reject(
          transactionEntry,
          rejection,
          commitContext.recordTime,
        )
    }
  }

  def validateConsistencyOfContracts(
      commitContext: CommitContext,
      transactionEntry: DamlTransactionEntrySummary,
      rejections: Rejections,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    val inputContracts: Map[Value.ContractId, DamlContractState] = commitContext
      .collectInputs {
        case (key, Some(value)) if value.hasContractState =>
          Conversions.stateKeyToContractId(key) -> value.getContractState
      }

    val areContractsConsistent = transactionEntry.transaction.inputContracts.forall(contractId =>
      !inputContracts(contractId).hasArchivedAt
    )

    if (areContractsConsistent)
      StepContinue(transactionEntry)
    else
      rejections.reject(
        transactionEntry,
        Rejection.ExternallyInconsistentTransaction.InconsistentContracts,
        commitContext.recordTime,
      )
  }

  private[validation] type RawContractId = String

  private[validation] sealed trait KeyValidationError extends Product with Serializable
  private[validation] case object Duplicate extends KeyValidationError
  private[validation] case object Inconsistent extends KeyValidationError
}
