// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive.codec

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
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
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.platform.store.dao.events.InputContractPackages
import com.digitalasset.canton.protocol.LfFatContractInst
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.HashingSchemeVersion
import com.digitalasset.daml.lf.transaction.{SubmittedTransaction, Transaction}
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

/** This class contains the logic to process prepare and execute requests from the interactive
  * submission API. The general flow is as follows:
  * {{{
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
  * }}}
  * Important to note is that input contracts' original data is passed back and forth between
  * prepare and execute, whereas the transaction itself is not. That's because it would become
  * increasingly difficult to maintain a correct enrich / impoverish logic over arbitrary old input
  * contracts with different Lf encodings. The downside is increased payload size for the prepared
  * transaction that now contains the input contracts in both enriched and original form. For
  * transactions we can tie the enrich / impoverish to the hashing scheme version and ensure that
  * the roundtrip is injective within the same version. Prepared transaction also have a much
  * shorter lifetime than input contracts in general so re-preparing a transaction with a newer
  * hashing version after an upgrade is relatively cheap.
  */
class ExternalTransactionProcessor(
    enricher: InteractiveSubmissionEnricher,
    contractStore: ContractStore,
    syncService: state.SyncService,
    config: InteractiveSubmissionServiceConfig,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val encoder = new PreparedTransactionEncoder(loggerFactory)
  private val decoder = new PreparedTransactionDecoder(loggerFactory)

  private def lookupAndEnrichInputContracts(
      transaction: Transaction,
      disclosedContracts: Map[ContractId, LfFatContractInst],
      contractLookupParallelism: PositiveInt,
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Map[ContractId, ExternalInputContract]] = {

    def lookupContract(coid: ContractId): FutureUnlessShutdown[LfFatContractInst] =
      disclosedContracts.get(coid) match {
        case Some(inst) =>
          FutureUnlessShutdown.pure(inst)
        case None =>
          FutureUnlessShutdown
            .outcomeF(contractStore.lookupContractState(coid))
            .flatMap[LfFatContractInst] {

              case active: ContractState.Active =>
                FutureUnlessShutdown.pure(active.contractInstance)

              // Engine interpretation likely would have failed if that was the case
              // However it's possible that the contract was archived or pruned in the meantime
              // That's not an issue however because if that was the case the transaction would have failed later
              // anyway during conflict detection.
              case ContractState.NotFound =>
                FutureUnlessShutdown
                  .failed(
                    ConsistencyErrors.ContractNotFound
                      .Reject(
                        s"Contract was not found in the participant contract store. You must either explicitly disclose the contract, or prepare the transaction via a participant that has knowledge of it",
                        coid,
                      )
                      .asGrpcError
                  )
              case ContractState.Archived =>
                FutureUnlessShutdown
                  .failed(
                    CommandExecutionErrors.Interpreter.ContractNotActive
                      .Reject(
                        "Input contract has seemingly already been archived immediately after interpretation of the transaction",
                        coid,
                        None,
                      )
                      .asGrpcError
                  )
            }
      }

    MonadUtil
      .parTraverseWithLimit(contractLookupParallelism)(
        InputContractPackages.forTransaction(transaction).toList
      ) { case (inputCoid, targetPackageIds) =>
        for {
          original <- EitherT.right[String](lookupContract(inputCoid))
          enriched <- enricher.enrichContract(original, targetPackageIds)
        } yield {
          inputCoid -> ExternalInputContract(
            originalContract = original,
            enrichedContract = enriched,
          )
        }
      }
      .map(_.toMap)

  }

  private def enrich(
      commandExecutionResult: CommandExecutionResult,
      disclosedContracts: Seq[DisclosedContract],
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
      enrichedTransaction <- EitherT.liftF(
        enricher.enrichVersionedTransaction(
          commandExecutionResult.commandInterpretationResult.transaction
        )
      )
      disclosedContractMap <- EitherT.fromEither[FutureUnlessShutdown](
        MapsUtil
          .toNonConflictingMap(
            disclosedContracts.map(_.fatContractInstance).map(c => c.contractId -> c)
          )
          .leftMap(err =>
            CommandExecutionErrors.InteractiveSubmissionPreparationError.Reject(
              s"Disclosed contracts contain non-unique contract IDs: $err"
            )
          )
      )
      // Compute input contracts by looking them up either from disclosed contracts or the local store
      inputContracts <- lookupAndEnrichInputContracts(
        enrichedTransaction.transaction,
        disclosedContractMap,
        contractLookupParallelism,
      )
        .leftMap(CommandExecutionErrors.InteractiveSubmissionPreparationError.Reject(_))
      // The participant needs to be connected to this synchronizer ID for the transaction to be submitted successfully
      synchronizerId = commandExecutionResult.synchronizerRank.synchronizerId
      transactionData = PrepareTransactionData(
        submitterInfo = commandExecutionResult.commandInterpretationResult.submitterInfo,
        transactionMeta = commandExecutionResult.commandInterpretationResult.transactionMeta,
        transaction = SubmittedTransaction(enrichedTransaction),
        globalKeyMapping = commandExecutionResult.commandInterpretationResult.globalKeyMapping,
        inputContracts = inputContracts,
        synchronizerId = synchronizerId.logical,
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
      enriched <- enrich(
        commandExecutionResult,
        commands.disclosedContracts.toList,
        contractLookupParallelism,
        maxRecordTime,
      )
      // Then encode
      encoded <- EitherT
        .liftF[
          Future,
          InteractiveSubmissionPreparationError.Reject,
          PreparedTransaction,
        ](encoder.encode(enriched))
        .mapK(FutureUnlessShutdown.outcomeK)
      // Compute the pre-computed hash for convenience
      protocolVersion = commandExecutionResult.synchronizerRank.synchronizerId.protocolVersion
      hashVersion = HashingSchemeVersion
        .getHashingSchemeVersionsForProtocolVersion(protocolVersion)
        .max1
      hash <- EitherT
        .fromEither[FutureUnlessShutdown](
          enriched
            .computeHash(hashVersion, protocolVersion, hashTracer)
            .leftMap(error => InteractiveSubmissionPreparationError.Reject(error.message))
        )
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
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        !config.enforceSingleRootNode || executeRequest.preparedTransaction.transaction
          .forall(_.roots.sizeIs == 1),
        InteractiveSubmissionExecuteError.Reject(
          "Transaction with multiple root nodes are not supported"
        ),
      )
      routingSynchronizerState <- EitherT.liftF(syncService.getRoutingSynchronizerState)
      decoded <- EitherT
        .liftF[Future, InteractiveSubmissionExecuteError.Reject, ExecuteTransactionData](
          decoder.decode(executeRequest)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      _ <- decoded
        .verifySignature(routingSynchronizerState, logger)
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
      commandInterpretationResult,
      selectRoutingSynchronizer,
      routingSynchronizerState,
    )
}
