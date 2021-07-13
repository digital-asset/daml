// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.validation

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractKey,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.validation.KeyMonotonicityValidation.checkContractKeysCausalMonotonicity
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Step,
  Rejections,
}
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.Transaction.{
  DuplicateKeys,
  InconsistentKeys,
  KeyActive,
  KeyCreate,
  NegativeKeyLookup,
}
import com.daml.logging.LoggingContext

private[transaction] object ContractKeysValidator extends TransactionValidator {

  /** Creates a committer step that validates casual monotonicity and consistency of contract keys
    * against the current ledger state.
    */
  override def createValidationStep(rejections: Rejections): Step = new Step {
    def apply(
        commitContext: CommitContext,
        transactionEntry: DamlTransactionEntrySummary,
    )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
      val damlState = commitContext
        .collectInputs[(DamlStateKey, DamlStateValue), Map[DamlStateKey, DamlStateValue]] {
          case (key, Some(value)) if key.hasContractKey => key -> value
        }
      val contractKeyDamlStateKeysToContractIds: Map[DamlStateKey, RawContractId] =
        damlState.collect {
          case (k, v) if k.hasContractKey && v.getContractKeyState.getContractId.nonEmpty =>
            k -> v.getContractKeyState.getContractId
        }
      // State before the transaction
      val contractKeyDamlStateKeys: Set[DamlStateKey] =
        contractKeyDamlStateKeysToContractIds.keySet
      val contractKeysToContractIds: Map[DamlContractKey, RawContractId] =
        contractKeyDamlStateKeysToContractIds.map(m => m._1.getContractKey -> m._2)

      for {
        stateAfterMonotonicityCheck <- checkContractKeysCausalMonotonicity(
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          damlState,
          transactionEntry,
          rejections,
        )
        finalState <- performTraversalContractKeysChecks(
          commitContext.recordTime,
          contractKeysToContractIds,
          stateAfterMonotonicityCheck,
          rejections,
        )
      } yield finalState
    }
  }

  private def performTraversalContractKeysChecks(
      recordTime: Option[Timestamp],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      transactionEntry: DamlTransactionEntrySummary,
      rejections: Rejections,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    import scalaz.std.either._
    import scalaz.std.list._
    import scalaz.syntax.foldable._

    val transaction = transactionEntry.transaction

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
        val message = error match {
          case Duplicate =>
            "DuplicateKeys: at least one contract key is not unique"
          case Inconsistent =>
            "InconsistentKeys: at least one contract key has changed since the submission"
        }
        rejections.buildRejectionStep(
          rejections.buildRejectionEntry(
            transactionEntry,
            RejectionReasonV0.Inconsistent(message),
          ),
          recordTime,
        )
    }
  }

  private[validation] type RawContractId = String

  private[validation] sealed trait KeyValidationError extends Product with Serializable
  private[validation] case object Duplicate extends KeyValidationError
  private[validation] case object Inconsistent extends KeyValidationError
}
