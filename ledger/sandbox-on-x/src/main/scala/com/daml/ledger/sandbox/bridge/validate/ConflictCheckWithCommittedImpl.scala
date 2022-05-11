// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge.validate

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{IndexService, MaximumLedgerTime}
import ConflictCheckingLedgerBridge._
import com.daml.ledger.participant.state.v2.{CompletionInfo, DisclosedContract}
import com.daml.ledger.sandbox.bridge.BridgeMetrics
import com.daml.ledger.sandbox.domain.Rejection._
import com.daml.ledger.sandbox.domain.Submission.Transaction
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{Transaction => LfTransaction}
import com.daml.lf.value.Value
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
            validateExplicitDisclosure(
              originalSubmission.submitterInfo.explicitDisclosure,
              originalSubmission.loggingContext,
              originalSubmission.submitterInfo.toCompletionInfo(),
            ).flatMap {
              case Right(_) =>
                validateCausalMonotonicity(
                  transaction = originalSubmission,
                  inputContracts = inputContracts,
                  transactionLedgerEffectiveTime =
                    originalSubmission.transactionMeta.ledgerEffectiveTime,
                  divulged = blindingInfo.divulgence.keySet,
                )
              case rejection => Future.successful(rejection)
            }.flatMap {
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

  private def sameContractData(
      actual: (Value.VersionedContractInstance, Timestamp),
      provided: DisclosedContract,
  )(implicit loggingContext: LoggingContext): Boolean = {
    // TODO DPP-1026: Note: Not validating transaction version or agreement text here, because we're making those up

    // TODO DPP-1026: Template id validation can probably be removed once we switch from VersionedContractInstance to Value
    val actualTemplate = actual._1.unversioned.template
    val providedTemplate = provided.contractInst.unversioned.template

    // TODO DPP-1026: Do we need the user to provide verbose arguments?
    // Or can we pass around explicit disclosure contract payloads without type information?
    val actualArgument = removeTypeInfo(actual._1.unversioned.arg)
    val providedArgument = removeTypeInfo(provided.contractInst.unversioned.arg)

    if (actualTemplate != providedTemplate) {
      // TODO DPP-1026: fix the logging context, it should include at least the submission id (to track malicious users).
      logger.warn(s"Disclosed contract ${provided.contractId.coid} has invalid template id")
      false
    } else if (actualArgument != providedArgument) {
      logger.warn(s"Disclosed contract ${provided.contractId.coid} has invalid argument")
      false
    } else if (actual._2 != provided.ledgerEffectiveTime) {
      logger.warn(s"Disclosed contract ${provided.contractId.coid} has invalid ledgerEffectiveTime")
      false
    } else {
      true
    }
  }

  // TODO DPP-1026: Move to package com.daml.lf
  def removeTypeInfo(a: Value): Value = {
    a match {
      case Value.ValueInt64(_) => a
      case Value.ValueNumeric(_) => a
      case Value.ValueText(_) => a
      case Value.ValueTimestamp(_) => a
      case Value.ValueParty(_) => a
      case Value.ValueBool(_) => a
      case Value.ValueDate(_) => a
      case Value.ValueUnit => a
      case Value.ValueContractId(_) => a
      case Value.ValueRecord(_, fields) =>
        Value.ValueRecord(None, fields.map(t => None -> t._2))
      case Value.ValueVariant(_, variant, value) =>
        Value.ValueVariant(None, variant, value)
      case Value.ValueEnum(_, value) =>
        Value.ValueEnum(None, value)
      case Value.ValueList(values) =>
        Value.ValueList(values.map(removeTypeInfo))
      case Value.ValueOptional(value) =>
        Value.ValueOptional(value.map(removeTypeInfo))
      case Value.ValueTextMap(map) =>
        Value.ValueTextMap(map.mapValue(removeTypeInfo))
      case Value.ValueGenMap(entries) =>
        Value.ValueGenMap(entries.map(t => removeTypeInfo(t._1) -> removeTypeInfo(t._2)))
    }
  }

  private def validateExplicitDisclosure(
      disclosedContracts: Set[DisclosedContract],
      loggingContext: LoggingContext,
      completionInfo: CompletionInfo,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): AsyncValidation[Unit] = {
    disclosedContracts.foldLeft(Future.successful[Validation[Unit]](Right(()))) {
      case (f, provided) =>
        f.flatMap {
          case Right(_) =>
            indexService
              .lookupContractAfterInterpretation(provided.contractId)(loggingContext)
              .map {
                case None =>
                  Left(
                    // Disclosed contract was archived or never existed
                    // This has intentionally the same error code as the cases below,
                    // to make sure malicious users cannot learn about the activeness of a third party contract
                    // by faking the contract stakeholders to get around Daml authorization rules.
                    DisclosedContractInvalid(provided.contractId, completionInfo)
                  )
                case Some(actual) if !sameContractData(actual, provided)(loggingContext) =>
                  Left(
                    // Disclosed contract has a bad payload, most likely submitted by a malicious user
                    DisclosedContractInvalid(provided.contractId, completionInfo)
                  )
                case _ => Right(())
              }
          case left => Future.successful(left)
        }
    }
  }
}
