// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import ConflictCheckingLedgerBridge._
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.sandbox.bridge.{
  BridgeMetrics,
  PreparedSubmission,
  PreparedTransactionSubmission,
}
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.Transaction
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Conflict checking for incoming submissions against the ledger state
  * as it is visible on the Ledger API.
  */
private[validate] class ConflictCheckWithCommittedImpl(
    indexService: IndexService,
    bridgeMetrics: BridgeMetrics,
    errorFactories: ErrorFactories,
)(implicit executionContext: ExecutionContext)
    extends ConflictCheckWithCommitted {
  private[this] implicit val logger: ContextualizedLogger = ContextualizedLogger.get(getClass)

  override def apply(
      in: Validation[(Offset, PreparedSubmission)]
  ): AsyncValidation[(Offset, PreparedSubmission)] = in match {
    case Left(rejection) => Future.successful(Left(rejection))
    case Right(
          validated @ (
            _,
            PreparedTransactionSubmission(
              keyInputs,
              inputContracts,
              _,
              _,
              blindingInfo,
              transactionInformees,
              originalSubmission,
            ),
          )
        ) =>
      withErrorLogger(originalSubmission.submitterInfo.submissionId) { implicit errorLogger =>
        Timed
          .future(
            bridgeMetrics.Stages.conflictCheckWithCommitted,
            validateCausalMonotonicity(
              transaction = originalSubmission,
              inputContracts = inputContracts,
              transactionLedgerEffectiveTime =
                originalSubmission.transactionMeta.ledgerEffectiveTime,
              divulged = blindingInfo.divulgence.keySet,
            ).flatMap {
              case Right(_) =>
                validateKeyUsages(
                  transactionInformees,
                  keyInputs,
                  originalSubmission.loggingContext,
                  originalSubmission.submitterInfo.toCompletionInfo(),
                )
              case rejection => Future.successful(rejection)
            },
          )
          .map(_.map(_ => validated))
      }(originalSubmission.loggingContext, logger)
    case Right(validated) => Future.successful(Right(validated))
  }

  private def validateCausalMonotonicity(
      transaction: Transaction,
      inputContracts: Set[ContractId],
      transactionLedgerEffectiveTime: Timestamp,
      divulged: Set[ContractId],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] = {
    val referredContracts = inputContracts.diff(divulged)
    val completionInfo = transaction.submitterInfo.toCompletionInfo()
    if (referredContracts.isEmpty)
      Future.successful(Right(()))
    else
      indexService
        .lookupMaximumLedgerTime(referredContracts)(transaction.loggingContext)
        .transform {
          case Failure(MissingContracts(missingContractIds)) =>
            Success(Left(UnknownContracts(missingContractIds)(completionInfo, errorFactories)))
          case Failure(err) =>
            Success(Left(LedgerBridgeInternalError(err, completionInfo)))
          case Success(maximumLedgerEffectiveTime) =>
            maximumLedgerEffectiveTime
              .filter(_ > transactionLedgerEffectiveTime)
              .fold[Try[Validation[Unit]]](Success(Right(())))(contractLedgerEffectiveTime =>
                Success(
                  Left(
                    CausalMonotonicityViolation(
                      contractLedgerEffectiveTime = contractLedgerEffectiveTime,
                      transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                    )(completionInfo, errorFactories)
                  )
                )
              )
        }
  }

  private def validateKeyUsages(
      transactionInformees: Set[Ref.Party],
      keyInputs: KeyInputs,
      loggingContext: LoggingContext,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] = {
    keyInputs.foldLeft(Future.successful[Validation[Unit]](Right(()))) {
      case (f, (key, inputState)) =>
        f.flatMap {
          case Right(_) =>
            indexService
              // TODO SoX: Perform lookup more efficiently and do not use a readers-based lookup
              .lookupContractKey(transactionInformees, key)(loggingContext)
              .map { lookupResult =>
                (inputState, lookupResult) match {
                  case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
                    Left(
                      InconsistentContractKey(None, Some(actual))(completionInfo, errorFactories)
                    )
                  case (LfTransaction.KeyCreate, Some(_)) =>
                    Left(DuplicateKey(key)(completionInfo, errorFactories))
                  case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
                    Left(
                      InconsistentContractKey(Some(expected), actual)(
                        completionInfo,
                        errorFactories,
                      )
                    )
                  case _ => Right(())
                }
              }
          case left => Future.successful(left)
        }
    }
  }

}
