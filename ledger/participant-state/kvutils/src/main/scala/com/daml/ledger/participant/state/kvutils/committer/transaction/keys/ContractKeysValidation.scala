// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer.transaction.keys

import com.daml.ledger.participant.state.kvutils.Conversions
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractKey,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.{
  DamlTransactionEntrySummary,
  Step,
  TransactionCommitter,
}
import com.daml.ledger.participant.state.kvutils.committer.transaction.keys.KeyMonotonicityValidation.checkContractKeysCausalMonotonicity
import com.daml.ledger.participant.state.kvutils.committer.{CommitContext, StepContinue, StepResult}
import com.daml.ledger.participant.state.v1.RejectionReasonV0
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext

private[transaction] object ContractKeysValidation {
  def validateKeys(transactionCommitter: TransactionCommitter): Step = new Step {
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
          transactionCommitter,
          commitContext.recordTime,
          contractKeyDamlStateKeys,
          damlState,
          transactionEntry,
        )
        finalState <- performTraversalContractKeysChecks(
          transactionCommitter,
          commitContext.recordTime,
          contractKeysToContractIds,
          stateAfterMonotonicityCheck,
        )
      } yield finalState
    }
  }

  private def performTraversalContractKeysChecks(
      transactionCommitter: TransactionCommitter,
      recordTime: Option[Timestamp],
      contractKeysToContractIds: Map[DamlContractKey, RawContractId],
      transactionEntry: DamlTransactionEntrySummary,
  )(implicit loggingContext: LoggingContext): StepResult[DamlTransactionEntrySummary] = {
    import scalaz.std.either._
    import scalaz.std.list._
    import scalaz.syntax.foldable._

    import com.daml.lf.transaction.Transaction.{
      KeyActive,
      KeyCreate,
      NegativeKeyLookup,
      DuplicateKeys,
      InconsistentKeys,
    }

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
        transactionCommitter.reject(
          recordTime,
          transactionCommitter.buildRejectionLogEntry(
            transactionEntry,
            RejectionReasonV0.Inconsistent(message),
          ),
        )
    }
  }

  private[keys] type RawContractId = String

  private[keys] sealed trait KeyValidationError extends Product with Serializable
  private[keys] case object Duplicate extends KeyValidationError
  private[keys] case object Inconsistent extends KeyValidationError
}
