// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive.codec

import cats.data.EitherT
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfTimestamp
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.interactive.InteractiveSubmissionEnricher
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.ExecuteRequest
import com.digitalasset.canton.ledger.api.{Commands as ApiCommands, DisclosedContract}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.{
  InteractiveSubmissionExecuteError,
  InteractiveSubmissionPreparationError,
}
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.SynchronizerRank
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStore}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.apiserver.execution.CommandExecutionResult
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.EnrichedTransactionData.ExternalInputContract
import com.digitalasset.canton.platform.apiserver.services.command.interactive.codec.ExternalTransactionProcessor.PrepareResult
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.Traced
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

object ExternalTransactionProcessor {
  final case class PrepareResult(
      transaction: PreparedTransaction,
      hash: Hash,
      hashVersion: HashingSchemeVersion,
  )
}

// format: off
/** This class contains the logic to processes prepare and execute requests from the interactive
 * submission API. The general flow is as follows:
 *
 * IC = Input Contract
 *
 *                                          ┌───────────────┐         ┌──────────────────────┐           ExternalHash = Hash(EnrichedLfTx, EnrichedIC)
 *                                          │     LfTx      │         │     EnrichedLfTx     │           Sign(ExternalHash)
 *          ┌─────────┐  Interpretation     ├───────────────┤ Enrich  ├──────────────────────┤ Encode   ┌─────────────────────┐
 * Prepare: │ Command ┼────────────────────►│  Original IC  │────────►│EnrichedIC, OriginalIC│─────────►│ PreparedTransaction │
 *          └─────────┘                     └───────────────┘         └──────────────────────┘          └───────────────────┬─┘
 *                                                 ||                                      ||                               │
 *                                          Equal  ||                               Equal  ||                               │
 *                                                 ||                                      ||                               │
 *                         Submit to Sync   ┌───────────────┐                         ┌──────────────────────┐   Decode     │
 * Execute:              ◄──────────────────│     LfTx      │                         │     EnrichedLfTx     │◄─────────────┘
 *                                          ├───────────────┤                         ├──────────────────────┤
 *                                          │  Original IC  │                         │EnrichedIC, OriginalIC│
 *                                          └───────────────┘                         └──────────┬───────────┘
 *                                                   ▲                                           │
 *                                                   │                                           │
 *                                                   │                           VerifySignature(EnrichedTx, EnrichedIC)
 *                                       Impoverish(EnrichedLfTx)                                │
 *                                                   │                                           │
 *                                                   │                                           │
 *                                                   └───────────────────────◄───────────────────┘
 *
 * Important to note is that input contracts' original data is passed back and forth between prepare and execute, whereas the transaction itself is not.
 * That's because it would become increasingly difficult to maintain a correct enrich / impoverish logic over arbitrary old input contracts with different Lf encodings.
 * The downside is increased payload size for the prepared transaction that now contains the input contracts in both enriched and original form.
 * For transactions we can tie the enrich / impoverish to the hashing scheme version and ensure that the roundtrip is injective within the same version.
 * Prepared transaction also have a much shorter lifetime than input contracts in general so re-preparing a transaction with a newer hashing version
 * after an upgrade is relatively cheap.
 */
// format: on
class ExternalTransactionProcessor(
    enricher: InteractiveSubmissionEnricher,
    contractStore: ContractStore,
    syncService: state.SyncService,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)

  private def lookupAndEnrichInputContracts(
      transaction: Transaction,
      disclosedContracts: Seq[DisclosedContract],
      contractLookupParallelism: PositiveInt,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Map[ContractId, ExternalInputContract]] = {
    val disclosedContractsByCoid =
      disclosedContracts.groupMap(_.fatContractInstance.contractId)(_.fatContractInstance)

    def enrich(
        instance: FatContractInstance
    ): FutureUnlessShutdown[FatContractInstance] =
      enricher.enrichContract(instance)

    MonadUtil
      .parTraverseWithLimit(contractLookupParallelism)(transaction.inputContracts.toList) {
        inputCoid =>
          // First check the disclosed contracts
          disclosedContractsByCoid.get(inputCoid) match {
            // We expect a single disclosed contract for a coid
            case Some(Seq(originalFci)) =>
              EitherT.liftF[FutureUnlessShutdown, String, (ContractId, ExternalInputContract)](
                enrich(originalFci).map { enrichedFci =>
                  val externalInputContract = ExternalInputContract(
                    originalFci = originalFci,
                    enrichedFci = enrichedFci,
                  )
                  externalInputContract.contractId -> externalInputContract
                }
              )
            case Some(_) =>
              EitherT.leftT[FutureUnlessShutdown, (ContractId, ExternalInputContract)](
                s"Contract ID $inputCoid is not unique"
              )
            // If the contract is not disclosed, look it up from the store
            case None =>
              EitherT {
                FutureUnlessShutdown
                  .outcomeF(
                    contractStore
                      .lookupContractState(inputCoid)
                  )
                  .flatMap {
                    case active: ContractState.Active =>
                      val originalFci = active.toFatContractInstance(inputCoid)
                      enrich(originalFci)
                        .map { enrichedFci =>
                          val externalInputContract = ExternalInputContract(
                            originalFci = originalFci,
                            enrichedFci = enrichedFci,
                          )
                          externalInputContract.contractId -> externalInputContract
                        }
                        .map(Right(_))
                    // Engine interpretation likely would have failed if that was the case
                    // However it's possible that the contract was archived or pruned in the meantime
                    // That's not an issue however because if that was the case the transaction would have failed later
                    // anyway during conflict detection.
                    case ContractState.NotFound =>
                      FutureUnlessShutdown
                        .failed[Either[String, (ContractId, ExternalInputContract)]](
                          ConsistencyErrors.ContractNotFound
                            .Reject(
                              s"Contract was not found in the participant contract store. You must either explicitly disclose the contract, or prepare the transaction via a participant that has knowledge of it",
                              inputCoid,
                            )
                            .asGrpcError
                        )
                    case ContractState.Archived =>
                      FutureUnlessShutdown
                        .failed[Either[String, (ContractId, ExternalInputContract)]](
                          CommandExecutionErrors.Interpreter.ContractNotActive
                            .Reject(
                              "Input contract has seemingly already been archived immediately after interpretation of the transaction",
                              inputCoid,
                              None,
                            )
                            .asGrpcError
                        )
                  }
              }
          }
      }
      .map(_.toMap)
  }

  private def enrich(
      commandExecutionResult: CommandExecutionResult,
      commands: ApiCommands,
      contractLookupParallelism: PositiveInt,
      maxRecordTime: Option[LfTimestamp],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    InteractiveSubmissionPreparationError.Reject,
    PrepareTransactionData,
  ] =
    for {
      // First enrich the transaction
      enrichedTransaction <- EitherT
        .liftF(
          enricher
            .enrichVersionedTransaction(
              commandExecutionResult.commandInterpretationResult.transaction
            )
        )
      // Compute input contracts by looking them up either from disclosed contracts or the local store
      inputContracts <- lookupAndEnrichInputContracts(
        enrichedTransaction.transaction,
        commands.disclosedContracts.toList,
        contractLookupParallelism,
      )
        .leftMap(CommandExecutionErrors.InteractiveSubmissionPreparationError.Reject(_))
      // Require this participant to be connected to the synchronizer on which the transaction will be run
      synchronizerId = commandExecutionResult.synchronizerRank.synchronizerId
      transactionData = PrepareTransactionData(
        submitterInfo = commandExecutionResult.commandInterpretationResult.submitterInfo,
        transactionMeta = commandExecutionResult.commandInterpretationResult.transactionMeta,
        transaction = SubmittedTransaction(enrichedTransaction),
        globalKeyMapping = commandExecutionResult.commandInterpretationResult.globalKeyMapping,
        inputContracts = inputContracts,
        synchronizerId = synchronizerId,
        mediatorGroup = 0,
        transactionUUID = UUID.randomUUID(),
        maxRecordTime = maxRecordTime,
      )
    } yield transactionData

  /** Transform a newly interpreted transaction into a prepared transaction.
    */
  private[apiserver] def processPrepare(
      commandExecutionResult: CommandExecutionResult,
      commands: ApiCommands,
      contractLookupParallelism: PositiveInt,
      hashTracer: HashTracer,
      maxRecordTime: Option[LfTimestamp],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    InteractiveSubmissionPreparationError.Reject,
    PrepareResult,
  ] =
    for {
      // Enrich first
      enriched <- enrich(commandExecutionResult, commands, contractLookupParallelism, maxRecordTime)
      // Then encode
      encoded <- EitherT
        .liftF[
          Future,
          InteractiveSubmissionPreparationError.Reject,
          PreparedTransaction,
        ](encoder.encode(enriched))
        .mapK(FutureUnlessShutdown.outcomeK)
      // Compute the pre-computed hash for convenience
      protocolVersion <- EitherT
        .fromEither[FutureUnlessShutdown](protocolVersionForSynchronizerId(enriched.synchronizerId))
        .leftMap(error => InteractiveSubmissionPreparationError.Reject(error.cause))
      hashVersion = HashingSchemeVersion
        .getHashingSchemeVersionsForProtocolVersion(protocolVersion)
        .max1
      hash <- EitherT
        .fromEither[FutureUnlessShutdown](
          enriched.computeHash(hashVersion, protocolVersion, hashTracer)
        )
        .leftMap(error => InteractiveSubmissionPreparationError.Reject(error.message))
    } yield {
      PrepareResult(encoded, hash, hashVersion)
    }

  /** Decodes a prepared transaction, verify its signature and convert it to CommandExecutionResult
    */
  private[apiserver] def processExecute(executeRequest: ExecuteRequest)(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    InteractiveSubmissionExecuteError.Reject,
    CommandExecutionResult,
  ] =
    for {
      decoded <- EitherT
        .liftF[Future, InteractiveSubmissionExecuteError.Reject, ExecuteTransactionData](
          decoder.decode(executeRequest)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      routingSynchronizerState = syncService.getRoutingSynchronizerState
      protocolVersion <- EitherT
        .fromEither[FutureUnlessShutdown](protocolVersionForSynchronizerId(decoded.synchronizerId))
        .leftMap(error => InteractiveSubmissionExecuteError.Reject(error.cause))
      _ <- decoded
        .verifySignature(routingSynchronizerState, protocolVersion, logger)
        .leftMap(err => InteractiveSubmissionExecuteError.Reject(err))
      commandInterpretationResult = decoded.impoverish
      selectRoutingSynchronizer <- EitherT.liftF(
        syncService
          .selectRoutingSynchronizer(
            submitterInfo = commandInterpretationResult.submitterInfo,
            optSynchronizerId = commandInterpretationResult.optSynchronizerId,
            transactionMeta = commandInterpretationResult.transactionMeta,
            transaction = commandInterpretationResult.transaction,
            // We expect to have all input contracts explicitly disclosed here,
            // as we do not want the executing participant to use its local contracts when creating the views
            disclosedContractIds =
              commandInterpretationResult.processedDisclosedContracts.map(_.contractId).toList,
            transactionUsedForExternalSigning = true,
            routingSynchronizerState = routingSynchronizerState,
          )
          .map(FutureUnlessShutdown.pure)
          .leftMap(err => FutureUnlessShutdown.failed[SynchronizerRank](err.asGrpcError))
          .merge
          .flatten
      )
    } yield CommandExecutionResult(
      decoded.impoverish,
      selectRoutingSynchronizer,
      routingSynchronizerState,
    )

  private def protocolVersionForSynchronizerId(
      synchronizerId: SynchronizerId
  )(implicit loggingContext: LoggingContextWithTrace): Either[RpcError, ProtocolVersion] =
    syncService
      .getProtocolVersionForSynchronizer(Traced(synchronizerId))
      .toRight(
        CommandExecutionErrors.InteractiveSubmissionPreparationError
          .Reject(s"Unknown synchronizer id $synchronizerId")
      )
}
