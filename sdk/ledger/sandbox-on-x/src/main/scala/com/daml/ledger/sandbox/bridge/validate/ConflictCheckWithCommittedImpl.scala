// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{IndexService, MaximumLedgerTime}
import ConflictCheckingLedgerBridge._
import com.daml.ledger.participant.state.v2.CompletionInfo
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.Transaction
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Timed

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Conflict checking for incoming submissions against the ledger state
  * as it is visible on the Ledger API.
  */
private[validate] class ConflictCheckWithCommittedImpl(
    indexService: IndexService,
    bridgeMetrics: BridgeMetrics,
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
            bridgeMetrics.Stages.ConflictCheckWithCommitted.timer,
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
        .lookupMaximumLedgerTimeAfterInterpretation(referredContracts)(transaction.loggingContext)
        .transform {
          case Success(MaximumLedgerTime.Archived(missingContractIds)) =>
            Success(Left(UnknownContracts(missingContractIds)(completionInfo)))

          case Failure(err) =>
            Success(Left(LedgerBridgeInternalError(err, completionInfo)))

          case Success(MaximumLedgerTime.Max(maximumLedgerEffectiveTime))
              if maximumLedgerEffectiveTime > transactionLedgerEffectiveTime =>
            Success(
              Left(
                CausalMonotonicityViolation(
                  contractLedgerEffectiveTime = maximumLedgerEffectiveTime,
                  transactionLedgerEffectiveTime = transactionLedgerEffectiveTime,
                )(completionInfo)
              )
            )

          case Success(_) =>
            Success(Right(()))
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
              .lookupContractKey(transactionInformees, key)(loggingContext)
              .map { lookupResult =>
                (inputState, lookupResult) match {
                  case (LfTransaction.NegativeKeyLookup, Some(actual)) =>
                    Left(
                      InconsistentContractKey(None, Some(actual))(completionInfo)
                    )
                  case (LfTransaction.KeyCreate, Some(_)) =>
                    Left(DuplicateKey(key)(completionInfo))
                  case (LfTransaction.KeyActive(expected), actual) if !actual.contains(expected) =>
                    Left(
                      InconsistentContractKey(Some(expected), actual)(
                        completionInfo
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
