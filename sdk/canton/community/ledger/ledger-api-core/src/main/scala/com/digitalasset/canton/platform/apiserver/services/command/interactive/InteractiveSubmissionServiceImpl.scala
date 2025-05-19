// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command.interactive

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service as proto
import com.daml.scalautil.future.FutureConversion.CompletionStageConversionOps
import com.digitalasset.base.error.ErrorCode.LoggedApiException
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.crypto.InteractiveSubmission
import com.digitalasset.canton.crypto.InteractiveSubmission.TransactionMetadataForHashing
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService
import com.digitalasset.canton.ledger.api.services.InteractiveSubmissionService.{
  ExecuteRequest,
  PrepareRequest as PrepareRequestInternal,
  TransactionData,
}
import com.digitalasset.canton.ledger.api.{
  Commands as ApiCommands,
  DisclosedContract,
  PackageReference,
  SubmissionId,
}
import com.digitalasset.canton.ledger.error.groups.{CommandExecutionErrors, ConsistencyErrors}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.{ContractState, ContractStore}
import com.digitalasset.canton.ledger.participant.state.{SubmissionResult, SynchronizerRank}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.*
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.platform.PackagePreferenceBackend
import com.digitalasset.canton.platform.apiserver.SeedService
import com.digitalasset.canton.platform.apiserver.execution.{
  CommandExecutionResult,
  CommandExecutor,
}
import com.digitalasset.canton.platform.apiserver.services.{
  ErrorCause,
  RejectionGenerators,
  logging,
}
import com.digitalasset.canton.platform.config.InteractiveSubmissionServiceConfig
import com.digitalasset.canton.platform.store.dao.events.LfValueTranslation
import com.digitalasset.canton.protocol.hash.HashTracer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{MonadUtil, TryUtil}
import com.digitalasset.canton.version.{HashingSchemeVersion, ProtocolVersion}
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.PackageName
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value.ContractId
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

private[apiserver] object InteractiveSubmissionServiceImpl {

  def createApiService(
      submissionSyncService: state.SyncService,
      seedService: SeedService,
      commandExecutor: CommandExecutor,
      metrics: LedgerApiServerMetrics,
      checkOverloaded: TraceContext => Option[state.SubmissionResult],
      lfValueTranslation: LfValueTranslation,
      config: InteractiveSubmissionServiceConfig,
      contractStore: ContractStore,
      packagePreferenceBackend: PackagePreferenceBackend,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      tracer: Tracer,
  ): InteractiveSubmissionService & AutoCloseable = new InteractiveSubmissionServiceImpl(
    submissionSyncService,
    seedService,
    commandExecutor,
    metrics,
    checkOverloaded,
    lfValueTranslation,
    config,
    contractStore,
    packagePreferenceBackend,
    loggerFactory,
  )

}

private[apiserver] final class InteractiveSubmissionServiceImpl private[services] (
    syncService: state.SyncService,
    seedService: SeedService,
    commandExecutor: CommandExecutor,
    metrics: LedgerApiServerMetrics,
    checkOverloaded: TraceContext => Option[state.SubmissionResult],
    lfValueTranslation: LfValueTranslation,
    config: InteractiveSubmissionServiceConfig,
    contractStore: ContractStore,
    packagePreferenceService: PackagePreferenceBackend,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, tracer: Tracer)
    extends InteractiveSubmissionService
    with AutoCloseable
    with Spanning
    with NamedLogging {

  private val transactionEncoder = new PreparedTransactionEncoder(loggerFactory)
  private val transactionDecoder = new PreparedTransactionDecoder(loggerFactory)

  override def prepare(
      request: PrepareRequestInternal
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[proto.PrepareSubmissionResponse] =
    withEnrichedLoggingContext(logging.commands(request.commands)) { implicit loggingContext =>
      logger.info(
        s"Requesting preparation of daml transaction with command ID ${request.commands.commandId}"
      )
      val cmds = request.commands.commands.commands
      // TODO(i20726): make sure this does not leak information
      logger.debug(
        show"Submitted commands for prepare are: ${if (cmds.length > 1) "\n  " else ""}${cmds
            .map {
              case ApiCommand.Create(templateRef, _) =>
                s"create ${templateRef.qualifiedName}"
              case ApiCommand.Exercise(templateRef, _, choiceId, _) =>
                s"exercise @${templateRef.qualifiedName} $choiceId"
              case ApiCommand.ExerciseByKey(templateRef, _, choiceId, _) =>
                s"exerciseByKey @${templateRef.qualifiedName} $choiceId"
              case ApiCommand.CreateAndExercise(templateRef, _, choiceId, _) =>
                s"createAndExercise ${templateRef.qualifiedName} ... $choiceId ..."
            }
            .map(_.singleQuoted)
            .toSeq
            .mkString("\n  ")}"
      )

      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromOption(
          logger,
          loggingContext,
          request.commands.submissionId.map(SubmissionId.unwrap),
        )

      evaluateAndHash(seedService.nextSeed(), request.commands, request.verboseHashing)
    }

  private def lookupAndEnrichInputContracts(
      transaction: Transaction,
      disclosedContracts: Seq[DisclosedContract],
  )(implicit
      loggingContextWithTrace: LoggingContextWithTrace
  ): EitherT[Future, String, Map[ContractId, FatContractInstance]] = {
    val disclosedContractsByCoid =
      disclosedContracts.groupMap(_.fatContractInstance.contractId)(_.fatContractInstance)

    def enrich(instance: FatContractInstance) =
      lfValueTranslation.enrichContract(instance)
        .map(instance.contractId -> _)

    MonadUtil
      .parTraverseWithLimit(config.contractLookupParallelism)(transaction.inputContracts.toList) {
        inputCoid =>
          // First check the disclosed contracts
          disclosedContractsByCoid.get(inputCoid) match {
            // We expect a single disclosed contract for a coid
            case Some(Seq(instance)) =>
              EitherT.liftF[Future, String, (ContractId, FatContractInstance)](enrich(instance))
            case Some(_) =>
              EitherT.leftT[Future, (ContractId, FatContractInstance)](
                s"Contract ID $inputCoid is not unique"
              )
            // If the contract is not disclosed, look it up from the store
            case None =>
              EitherT {
                contractStore
                  .lookupContractState(inputCoid)
                  .flatMap {
                    case active: ContractState.Active =>
                      enrich(active.contractInstance).map(Right(_))
                    // Engine interpretation likely would have failed if that was the case
                    // However it's possible that the contract was archived or pruned in the meantime
                    // That's not an issue however because if that was the case the transaction would have failed later
                    // anyway during conflict detection.
                    case ContractState.NotFound =>
                      Future.failed[Either[String, (ContractId, FatContractInstance)]](
                        ConsistencyErrors.ContractNotFound
                          .Reject(
                            s"Contract was not found in the participant contract store. You must either explicitly disclose the contract, or prepare the transaction via a participant that has knowledge of it",
                            inputCoid,
                          )
                          .asGrpcError
                      )
                    case ContractState.Archived =>
                      Future.failed[Either[String, (ContractId, FatContractInstance)]](
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

  private def evaluateAndHash(
      submissionSeed: crypto.Hash,
      commands: ApiCommands,
      verboseHashing: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace,
      errorLoggingContext: ErrorLoggingContext,
  ): FutureUnlessShutdown[proto.PrepareSubmissionResponse] = {
    val result: EitherT[FutureUnlessShutdown, RpcError, proto.PrepareSubmissionResponse] = for {
      commandExecutionResult <- withSpan("InteractiveSubmissionService.evaluate") { _ => _ =>
        val synchronizerState = syncService.getRoutingSynchronizerState
        commandExecutor
          .execute(
            commands = commands,
            submissionSeed = submissionSeed,
            routingSynchronizerState = synchronizerState,
            forExternallySigned = true,
          )
          .leftFlatMap { errCause =>
            metrics.commands.failedCommandInterpretations.mark()
            EitherT.right[RpcError](failedOnCommandProcessing(errCause))
          }
      }

      preEnrichedCommandInterpretationResult = commandExecutionResult.commandInterpretationResult

      // We need to enrich the transaction before computing the hash, because record labels must be part of the hash
      enrichedTransaction <- EitherT
        .liftF(
          lfValueTranslation
            .enrichVersionedTransaction(preEnrichedCommandInterpretationResult.transaction)
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      // Compute input contracts by looking them up either from disclosed contracts or the local store
      inputContracts <- lookupAndEnrichInputContracts(
        enrichedTransaction.transaction,
        commands.disclosedContracts.toList,
      )
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap(CommandExecutionErrors.InteractiveSubmissionPreparationError.Reject(_))
      // Require this participant to be connected to the synchronizer on which the transaction will be run
      synchronizerId = commandExecutionResult.synchronizerRank.synchronizerId
      protocolVersionForChosenSynchronizer <- EitherT.fromEither[FutureUnlessShutdown](
        protocolVersionForSynchronizerId(synchronizerId)
      )

      transactionData = TransactionData(
        submitterInfo = commandExecutionResult.commandInterpretationResult.submitterInfo,
        transactionMeta = commandExecutionResult.commandInterpretationResult.transactionMeta,
        transaction = SubmittedTransaction(enrichedTransaction),
        globalKeyMapping = commandExecutionResult.commandInterpretationResult.globalKeyMapping,
        inputContracts = inputContracts,
        synchronizerId = synchronizerId,
      )

      // Use the highest hashing versions supported on that protocol version
      hashVersion = HashingSchemeVersion
        .getHashingSchemeVersionsForProtocolVersion(protocolVersionForChosenSynchronizer)
        .max1
      transactionUUID = UUID.randomUUID()
      mediatorGroup = 0
      preparedTransaction <- EitherT
        .liftF(
          transactionEncoder.serializeCommandInterpretationResult(
            transactionData,
            synchronizerId,
            transactionUUID,
            // TODO(i20688) Mediator group should be picked in the ProtocolProcessor
            mediatorGroup,
          )
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      metadataForHashing = TransactionMetadataForHashing.create(
        transactionData.submitterInfo.actAs.toSet,
        transactionData.submitterInfo.commandId,
        transactionUUID,
        mediatorGroup,
        synchronizerId,
        transactionData.transactionMeta.timeBoundaries,
        transactionData.transactionMeta.submissionTime,
        inputContracts,
      )
      hashTracer: HashTracer =
        if (config.enableVerboseHashing && verboseHashing)
          HashTracer.StringHashTracer(traceSubNodes = true)
        else
          HashTracer.NoOp
      transactionHash <- EitherT
        .fromEither[FutureUnlessShutdown](
          InteractiveSubmission.computeVersionedHash(
            hashVersion,
            transactionData.transaction,
            metadataForHashing,
            transactionData.transactionMeta.optNodeSeeds
              .map(_.toList.toMap)
              .getOrElse(Map.empty),
            protocolVersionForChosenSynchronizer,
            hashTracer,
          )
        )
        .leftMap(err =>
          CommandExecutionErrors.InteractiveSubmissionPreparationError
            .Reject(s"Failed to compute hash: $err"): RpcError
        )

      hashingDetails = hashTracer match {
        // If we have a NoOp tracer but verboseHashing was requested, it means it's disabled on the participant
        // Return a message to explain that
        case HashTracer.NoOp if verboseHashing =>
          Some(
            "Verbose hashing is disabled on this participant. Contact the node administrator for more details."
          )
        case HashTracer.NoOp => None
        case stringTracer: HashTracer.StringHashTracer => Some(stringTracer.result)
      }
    } yield proto.PrepareSubmissionResponse(
      preparedTransaction = Some(preparedTransaction),
      preparedTransactionHash = transactionHash.unwrap,
      hashingSchemeVersion = hashVersion.toLAPIProto,
      hashingDetails = hashingDetails,
    )

    result.value.map(_.leftMap(_.asGrpcError).toTry).flatMap(FutureUnlessShutdown.fromTry)
  }

  private def failedOnCommandProcessing(
      error: ErrorCause
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): FutureUnlessShutdown[CommandExecutionResult] =
    FutureUnlessShutdown.failed(
      RejectionGenerators
        .commandExecutorError(error)
        .asGrpcError
    )

  override def close(): Unit = ()

  private def submitIfNotOverloaded(transactionInfo: TransactionData)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[SubmissionResult] =
    checkOverloaded(loggingContext.traceContext) match {
      case Some(submissionResult) => Future.successful(submissionResult)
      case None => submitTransaction(transactionInfo)
    }

  private def submitTransaction(
      transactionInfo: TransactionData
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[state.SubmissionResult] = {
    metrics.commands.validSubmissions.mark()

    logger.trace("Submitting transaction to ledger.")

    val routingSynchronizerState = syncService.getRoutingSynchronizerState

    syncService
      .selectRoutingSynchronizer(
        submitterInfo = transactionInfo.submitterInfo,
        optSynchronizerId = Some(transactionInfo.synchronizerId),
        transactionMeta = transactionInfo.transactionMeta,
        transaction = transactionInfo.transaction,
        // We expect to have all input contracts explicitly disclosed here,
        // as we do not want the executing participant to use its local contracts when creating the views
        disclosedContractIds = transactionInfo.inputContracts.keys.toList,
        transactionUsedForExternalSigning = true,
        routingSynchronizerState = routingSynchronizerState,
      )
      .map(FutureUnlessShutdown.pure)
      .leftMap(err => FutureUnlessShutdown.failed[SynchronizerRank](err.asGrpcError))
      .merge
      .flatten
      .failOnShutdownTo(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
      .flatMap { synchronizerRank =>
        syncService
          .submitTransaction(
            transaction = transactionInfo.transaction,
            synchronizerRank = synchronizerRank,
            routingSynchronizerState = routingSynchronizerState,
            submitterInfo = transactionInfo.submitterInfo,
            transactionMeta = transactionInfo.transactionMeta,
            _estimatedInterpretationCost = 0L,
            keyResolver = transactionInfo.globalKeyMapping,
            processedDisclosedContracts = ImmArray.from(transactionInfo.inputContracts.values),
          )
          .toScalaUnwrapped
      }
  }

  private def protocolVersionForSynchronizerId(
      synchronizerId: SynchronizerId
  )(implicit loggingContext: LoggingContextWithTrace): Either[RpcError, ProtocolVersion] =
    syncService
      .getProtocolVersionForSynchronizer(Traced(synchronizerId))
      .toRight(
        CommandExecutionErrors.InteractiveSubmissionPreparationError
          .Reject(s"Unknown synchronizer id $synchronizerId")
      )

  private def handleSubmissionResult(result: Try[state.SubmissionResult])(implicit
      loggingContext: LoggingContextWithTrace
  ): Try[Unit] = {
    import state.SubmissionResult.*
    result match {
      case Success(Acknowledged) =>
        logger.debug("Interactive submission acknowledged by sync-service.")
        TryUtil.unit

      case Success(result: SynchronousError) =>
        logger.info(s"Rejected: ${result.description}")
        Failure(result.exception)

      // Do not log again on errors that are logging on creation
      case Failure(error: LoggedApiException) => Failure(error)
      case Failure(error) =>
        logger.info(s"Rejected: ${error.getMessage}")
        Failure(error)
    }
  }

  override def execute(
      executionRequest: ExecuteRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[proto.ExecuteSubmissionResponse] = FutureUnlessShutdown.outcomeF {
    val commandIdLogging =
      executionRequest.preparedTransaction.metadata
        .flatMap(_.submitterInfo.map(_.commandId))
        .map(logging.commandId)
        .toList

    withEnrichedLoggingContext(
      logging.submissionId(executionRequest.submissionId),
      commandIdLogging*
    ) { implicit loggingContext =>
      logger.info(
        s"Requesting execution of daml transaction with submission ID ${executionRequest.submissionId}"
      )
      for {
        deserializationResult <- transactionDecoder.deserialize(executionRequest)
        _ <- submitIfNotOverloaded(deserializationResult.preparedTransactionData)
          .transform(handleSubmissionResult)
      } yield proto.ExecuteSubmissionResponse()
    }
  }

  override def getPreferredPackageVersion(
      parties: Set[LfPartyId],
      packageName: PackageName,
      synchronizerId: Option[SynchronizerId],
      vettingValidAt: Option[CantonTimestamp],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): FutureUnlessShutdown[Option[(PackageReference, SynchronizerId)]] =
    packagePreferenceService.getPreferredPackageVersion(
      parties = parties,
      packageName = packageName,
      supportedPackageIds = None,
      synchronizerId = synchronizerId,
      vettingValidAt = vettingValidAt,
    )
}
