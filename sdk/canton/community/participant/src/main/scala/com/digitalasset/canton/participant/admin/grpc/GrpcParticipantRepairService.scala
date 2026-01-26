// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.base.error.RpcError
import com.digitalasset.canton.ProtoDeserializationError.{OtherError, ValueConversionError}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.ActiveContractOld.loadFromByteString
import com.digitalasset.canton.participant.admin.data.{
  ActiveContract,
  ContractImportMode,
  ManualLSURequest,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.{
  ValidExportAcsOldRequest,
  ValidExportAcsRequest,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceError
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  GrpcStreamingUtils,
  MonadUtil,
  OptionUtil,
  ResourceUtil,
}
import com.digitalasset.canton.{
  LfPartyId,
  ProtoDeserializationError,
  ReassignmentCounter,
  SequencerCounter,
  SynchronizerAlias,
  protocol,
}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.ActorSystem

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream
import scala.annotation.nowarn
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

final class GrpcParticipantRepairService(
    sync: CantonSyncService,
    parameters: ParticipantNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
) extends ParticipantRepairServiceGrpc.ParticipantRepairService
    with NamedLogging {

  private val synchronizerMigrationInProgress = new AtomicReference[Boolean](false)

  private val processingTimeout: ProcessingTimeout = parameters.processingTimeouts

  private val batching: BatchingConfig = parameters.batchingConfig

  /** purge contracts
    */
  override def purgeContracts(request: PurgeContractsRequest): Future[PurgeContractsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res: Either[RepairServiceError, Unit] = for {
      cids <- request.contractIds
        .traverse(LfContractId.fromString)
        .leftMap(RepairServiceError.InvalidArgument.Error(_))
      synchronizerAlias <- SynchronizerAlias
        .fromProtoPrimitive(request.synchronizerAlias)
        .leftMap(_.toString)
        .leftMap(RepairServiceError.InvalidArgument.Error(_))

      cidsNE <- NonEmpty
        .from(cids)
        .toRight(RepairServiceError.InvalidArgument.Error("Missing contract ids to purge"))

      _ <- sync.repairService
        .purgeContracts(synchronizerAlias, cidsNE, request.ignoreAlreadyPurged)
        .leftMap(RepairServiceError.ContractPurgeError.Error(synchronizerAlias, _))
    } yield ()

    res.fold(
      err => Future.failed(err.asGrpcError),
      _ => Future.successful(PurgeContractsResponse()),
    )
  }

  // TODO(#24610) – Remove, replaced by exportAcs
  @nowarn("cat=deprecation")
  override def exportAcsOld(
      request: ExportAcsOldRequest,
      responseObserver: StreamObserver[ExportAcsOldResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => createAcsSnapshotTemporaryFile(request, out),
      responseObserver,
      byteString => ExportAcsOldResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  // TODO(#24610) – Remove, replaced by exportAcs
  @nowarn("cat=deprecation")
  private def createAcsSnapshotTemporaryFile(
      request: ExportAcsOldRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val gzipOut = new GZIPOutputStream(out)
    val res = for {
      validRequest <- EitherT.fromEither[FutureUnlessShutdown](ValidExportAcsOldRequest(request))
      timestampAsString = validRequest.timestamp.fold("head")(ts => s"at $ts")
      _ = logger.info(
        s"Exporting active contract set ($timestampAsString) for parties ${validRequest.parties}"
      )
      _ <- ResourceUtil
        .withResourceM(gzipOut)(
          sync.stateInspection
            .exportAcsDumpActiveContracts(
              _,
              _.filterString.startsWith(request.filterSynchronizerId),
              validRequest.parties,
              validRequest.timestamp,
              skipCleanTimestampCheck = validRequest.force,
              partiesOffboarding = validRequest.partiesOffboarding,
            )
        )
        .leftMap(RepairServiceError.fromAcsInspectionError(_, logger))
    } yield ()

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  @nowarn("cat=deprecation")
  override def importAcsOld(
      responseObserver: StreamObserver[ImportAcsOldResponse]
  ): StreamObserver[ImportAcsOldRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val outputStream = new ByteArrayOutputStream()
    val args = new AtomicReference[Option[String]](None)
    def tryArgs: String =
      args
        .get()
        .getOrElse(throw new IllegalStateException("The import ACS request fields are not set"))

    new StreamObserver[ImportAcsOldRequest] {
      def setOrCheck(
          workflowIdPrefix: String
      ): Try[Unit] =
        Try {
          val newOrMatchingValue = Some(workflowIdPrefix)
          if (!args.compareAndSet(None, newOrMatchingValue)) {
            val oldWorkflowIdPrefix = tryArgs
            if (workflowIdPrefix != oldWorkflowIdPrefix) {
              throw new IllegalArgumentException(
                s"Workflow ID prefix cannot be changed from $oldWorkflowIdPrefix to $workflowIdPrefix"
              )
            }
          }
        }

      override def onNext(request: ImportAcsOldRequest): Unit = {
        val processRequest =
          for {
            _ <- setOrCheck(request.workflowIdPrefix)
            _ <- Try(outputStream.write(request.acsSnapshot.toByteArray))
          } yield ()

        processRequest match {
          case Failure(exception) =>
            outputStream.close()
            responseObserver.onError(exception)
          case Success(_) =>
            () // Nothing to do, just move on to the next request
        }
      }

      override def onError(t: Throwable): Unit = {
        responseObserver.onError(t)
        outputStream.close()
      }

      override def onCompleted(): Unit = {
        val workflowIdPrefix = tryArgs

        val res = importAcsSnapshotOld(
          data = ByteString.copyFrom(outputStream.toByteArray),
          workflowIdPrefix = workflowIdPrefix,
        )

        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(()) =>
            responseObserver.onNext(ImportAcsOldResponse())
            responseObserver.onCompleted()
        }
        outputStream.close()
      }
    }
  }

  private def importAcsSnapshotOld(
      data: ByteString,
      workflowIdPrefix: String,
  )(implicit traceContext: TraceContext): Future[Unit] =
    importAcsContractsOld(
      loadFromByteString(data).map(contracts => contracts.map(_.toRepairContract)),
      workflowIdPrefix,
    )

  override def exportAcs(
      request: v30.ExportAcsRequest,
      responseObserver: StreamObserver[v30.ExportAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => processExportAcs(request, new GZIPOutputStream(out)),
      responseObserver,
      byteString => v30.ExportAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def processExportAcs(
      request: v30.ExportAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val res = for {
      ledgerEnd <- EitherT
        .fromEither[FutureUnlessShutdown](ParticipantCommon.findLedgerEnd(sync))
        .leftMap(RepairServiceError.InvalidState.Error(_))
      allLogicalSynchronizerIds = sync.syncPersistentStateManager.getAllLatest.keySet

      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        validateExportAcsRequest(request, ledgerEnd, allLogicalSynchronizerIds)
      )
      indexService <- EitherT.fromOption[FutureUnlessShutdown](
        sync.internalIndexService,
        RepairServiceError.InvalidState.Error("Unavailable internal state service"),
      )

      snapshot <- ParticipantCommon
        .writeAcsSnapshot(
          indexService,
          validRequest.parties,
          validRequest.atOffset,
          out,
          validRequest.excludedStakeholders,
          validRequest.synchronizerId,
          validRequest.contractSynchronizerRenames,
        )(ec, traceContext, actorSystem)
        .leftMap(msg => RepairServiceError.IOStream.Error(msg): RepairServiceError)
    } yield snapshot

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  private def validateExportAcsRequest(
      request: v30.ExportAcsRequest,
      ledgerEnd: Offset,
      synchronizerIds: Set[SynchronizerId],
  )(implicit
      elc: ErrorLoggingContext
  ): Either[RepairServiceError, ValidExportAcsRequest] = {
    val parsingResult = for {
      parties <- request.partyIds.traverse(party =>
        UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_))
      )
      parsedFilterSynchronizerId <- OptionUtil
        .emptyStringAsNone(request.synchronizerId)
        .traverse(SynchronizerId.fromProtoPrimitive(_, "filter_synchronizer_id"))
      filterSynchronizerId <- Either.cond(
        parsedFilterSynchronizerId.forall(synchronizerIds.contains),
        parsedFilterSynchronizerId,
        OtherError(s"Filter synchronizer id $parsedFilterSynchronizerId is unknown"),
      )
      parsedOffset <- ProtoConverter
        .parseOffset("ledger_offset", request.ledgerOffset)
      ledgerOffset <- Either.cond(
        parsedOffset <= ledgerEnd,
        parsedOffset,
        OtherError(
          s"Ledger offset $parsedOffset needs to be smaller or equal to the ledger end $ledgerEnd"
        ),
      )
      contractSynchronizerRenames <- request.contractSynchronizerRenames.toList.traverse {
        case (source, v30.ExportAcsTargetSynchronizer(target)) =>
          for {
            _ <- SynchronizerId.fromProtoPrimitive(source, "source synchronizer id")
            _ <- SynchronizerId.fromProtoPrimitive(target, "target synchronizer id")
          } yield (source, target)
      }
      excludedStakeholders <- request.excludedStakeholderIds.traverse(party =>
        UniqueIdentifier.fromProtoPrimitive(party, "excluded_stakeholder_ids").map(PartyId(_))
      )

    } yield ValidExportAcsRequest(
      parties.toSet,
      ledgerOffset,
      excludedStakeholders.toSet,
      filterSynchronizerId,
      contractSynchronizerRenames.toMap,
    )
    parsingResult.leftMap(error => RepairServiceError.InvalidArgument.Error(error.message))
  }

  /*
   Note that `responseObserver` originates from `GrpcStreamingUtils.streamToServer` which is
   a wrapper that turns the responses into a promise/future. This is not a true bidirectional stream.
   */
  override def importAcs(
      responseObserver: StreamObserver[ImportAcsResponse]
  ): StreamObserver[ImportAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // TODO(#23818): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()

    // (workflowIdPrefix, ContractImportMode, excludedStakeholders, representativePackageIdOverride)
    type ImportArgs = (String, ContractImportMode, Set[PartyId], RepresentativePackageIdOverride)

    val args = new AtomicReference[Option[ImportArgs]](None)

    def recordedArgs: Either[String, ImportArgs] =
      args
        .get()
        .toRight("The import ACS request fields are not set")

    def setOrCheck(
        workflowIdPrefix: String,
        contractImportMode: ContractImportMode,
        excludeStakeholders: Set[PartyId],
        representativePackageIdOverride: RepresentativePackageIdOverride,
    ): Either[String, Unit] = {
      val newOrMatchingValue = Some(
        (workflowIdPrefix, contractImportMode, excludeStakeholders, representativePackageIdOverride)
      )
      if (args.compareAndSet(None, newOrMatchingValue)) {
        Right(()) // This was the first message, success, set.
      } else {
        recordedArgs.flatMap {
          case (oldWorkflowIdPrefix, _, _, _) if oldWorkflowIdPrefix != workflowIdPrefix =>
            Left(
              s"Workflow ID prefix cannot be changed from $oldWorkflowIdPrefix to $workflowIdPrefix"
            )
          case (_, oldContractImportMode, _, _) if oldContractImportMode != contractImportMode =>
            Left(
              s"Contract authentication import mode cannot be changed from $oldContractImportMode to $contractImportMode"
            )
          case (_, _, oldExcludedStakeholders, _)
              if oldExcludedStakeholders != excludeStakeholders =>
            Left(
              s"Exclude parties cannot be changed from $oldExcludedStakeholders to $excludeStakeholders"
            )
          case (_, _, _, oldRepresentativePackageIdOverride)
              if oldRepresentativePackageIdOverride != representativePackageIdOverride =>
            Left(
              s"Representative package ID override cannot be changed from $oldRepresentativePackageIdOverride to $representativePackageIdOverride"
            )

          case _ => Right(()) // All arguments matched successfully
        }
      }
    }

    new StreamObserver[ImportAcsRequest] {

      override def onNext(request: ImportAcsRequest): Unit = {
        val processRequest: Either[String, Unit] = for {
          contractImportMode <- ContractImportMode
            .fromProtoV30(request.contractImportMode)
            .leftMap(_.message)
          excludedStakeholders <- request.excludedStakeholderIds
            .traverse(party =>
              UniqueIdentifier
                .fromProtoPrimitive(party, "excluded_stakeholder_ids")
                .map(PartyId(_))
            )
            .leftMap(_.message)
          representativePackageIdOverrideO <- request.representativePackageIdOverride
            .traverse(RepresentativePackageIdOverride.fromProtoV30)
            .leftMap(_.message)
          _ <- setOrCheck(
            request.workflowIdPrefix,
            contractImportMode,
            excludedStakeholders.toSet,
            representativePackageIdOverrideO.getOrElse(RepresentativePackageIdOverride.NoOverride),
          )
        } yield ()

        processRequest.fold(
          // On failure: Signal the error, that is throw an exception.
          // Observer's top-level onError will handle cleanup.
          errorMessage => responseObserver.onError(new IllegalArgumentException(errorMessage)),
          _ => outputStream.write(request.acsSnapshot.toByteArray),
        )

      }

      override def onError(t: Throwable): Unit =
        try {
          responseObserver.onError(t)
        } finally {
          outputStream.close()
        }

      override def onCompleted(): Unit = {

        val result: EitherT[Future, Throwable, Unit] = for {

          argsTuple <- EitherT.fromEither[Future](
            recordedArgs.leftMap(new IllegalStateException(_))
          )
          (
            workflowIdPrefix,
            contractImportMode,
            excludedStakeholders,
            representativePackageIdOverride,
          ) = argsTuple

          acsSnapshot <- EitherT.fromEither[Future](
            Try(ByteString.copyFrom(outputStream.toByteArray)).toEither
          )

          _ <- EitherT.liftF[Future, Throwable, Unit](
            ParticipantCommon.importAcsNewSnapshot(
              acsSnapshot = acsSnapshot,
              workflowIdPrefix = workflowIdPrefix,
              contractImportMode = contractImportMode,
              excludedStakeholders = excludedStakeholders,
              representativePackageIdOverride = representativePackageIdOverride,
              sync = sync,
              batching = batching,
              alphaMultiSynchronizerSupport = parameters.alphaMultiSynchronizerSupport,
            )
          )
        } yield ()

        result
          .thereafter(_ => outputStream.close())
          .value // Get the underlying Future[Either[...]]
          .onComplete {
            // The Future itself failed (e.g., a fatal error in `thereafter`)
            case Failure(exception) =>
              responseObserver.onError(exception)

            case Success(result) =>
              result match {
                case Left(exception) =>
                  responseObserver.onError(exception)
                case Right(()) =>
                  responseObserver.onNext(ImportAcsResponse())
                  responseObserver.onCompleted()
              }
          }
      }
    }
  }

  // TODO(#30342) - Consolidate with importAcs, or separate it clearly
  override def importAcsV2(
      responseObserver: StreamObserver[ImportAcsV2Response]
  ): StreamObserver[ImportAcsV2Request] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    type ImportArgs =
      (
          Option[String],
          ContractImportMode,
          Set[LfPartyId],
          RepresentativePackageIdOverride,
          SynchronizerId,
      )

    def extractImportArgs(
        request: ImportAcsV2Request
    ): Try[ImportArgs] = {
      val resultE = for {
        contractImportMode <- ProtoConverter.parseRequired(
          ContractImportMode.fromProtoV30,
          "contract_import_mode",
          request.contractImportMode,
        )
        excludedStakeholders <- request.excludedStakeholderIds
          .traverse(party =>
            UniqueIdentifier
              .fromProtoPrimitive(party, "excluded_stakeholder_ids")
              .map(PartyId(_).toLf)
          )
        representativePackageIdOverrideO <- request.representativePackageIdOverride
          .traverse(RepresentativePackageIdOverride.fromProtoV30)
        synchronizerId <- ProtoConverter.parseRequired(
          SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"),
          "synchronizer_id",
          request.synchronizerId,
        )
      } yield (
        request.workflowIdPrefix,
        contractImportMode,
        excludedStakeholders.toSet,
        representativePackageIdOverrideO.getOrElse(RepresentativePackageIdOverride.NoOverride),
        synchronizerId,
      )

      resultE
        .leftMap(ProtoDeserializationError.ProtoDeserializationFailure.Wrap(_).asGrpcError)
        .toTry
    }

    GrpcStreamingUtils
      .streamGzippedChunksFromClient[
        ImportAcsV2Request,
        ImportAcsV2Response,
        ImportArgs,
        ActiveContract,
      ](
        responseObserver,
        Success(ImportAcsV2Response()),
        getGzippedBytes = _.acsSnapshot,
        parseMessage = ActiveContract.parseDelimitedFromTrusted(_),
      )(contextFromFirstRequest = extractImportArgs) {
        case (
              (
                workFlowIdPrefix,
                contractImportMode,
                excludedStakeholders,
                representativePackageIdOverride,
                synchronizerId,
              ),
              source,
            ) =>
          val repairSource = source
            .map(activeContract =>
              RepairContract
                .fromLapiActiveContract(activeContract.contract)
                .valueOr(err =>
                  throw ProtoDeserializationError.ProtoDeserializationFailure
                    .Wrap(ProtoDeserializationError.ValueConversionError("contract", err))
                    .asGrpcError
                )
            )

          val filteredSource = if (excludedStakeholders.isEmpty) {
            repairSource
          } else {
            repairSource.filter(
              _.contract.stakeholders.intersect(excludedStakeholders).isEmpty
            )
          }
          val resultEUS = sync.repairService.addContractsPekko(
            synchronizerId,
            filteredSource,
            contractImportMode,
            sync.getPackageMetadataSnapshot,
            representativePackageIdOverride,
            workflowIdPrefix = workFlowIdPrefix,
          )
          EitherTUtil.toFutureUnlessShutdown(
            resultEUS.bimap(
              err => RepairServiceError.ImportAcsError.Error(err).asGrpcError,
              _ => ImportAcsV2Response(),
            )
          )

      }
  }

  private def importAcsContractsOld(
      contracts: Either[String, List[RepairContract]],
      workflowIdPrefix: String,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val resultET = for {
      repairContracts <- contracts
        .toEitherT[FutureUnlessShutdown]
        .ensure(
          "Found at least one contract with a non-zero reassignment counter. ACS import does not yet support it."
        )(_.forall(_.reassignmentCounter == ReassignmentCounter.Genesis))

      workflowIdPrefixO = Option.when(workflowIdPrefix != "")(workflowIdPrefix)

      _ <- repairContracts.groupBy(_.synchronizerId).toSeq.parTraverse_ {
        case (synchronizerId, contracts) =>
          MonadUtil.batchedSequentialTraverse_(
            batching.parallelism,
            batching.maxAcsImportBatchSize,
          )(contracts)(
            writeContractsBatchOld(workflowIdPrefixO)(synchronizerId, _)
              .mapK(FutureUnlessShutdown.outcomeK)
          )
      }

    } yield ()

    resultET.value.flatMap {
      case Left(error) => FutureUnlessShutdown.failed(ImportAcsError.Error(error).asGrpcError)
      case Right(()) => FutureUnlessShutdown.unit
    }.asGrpcFuture
  }

  private def writeContractsBatchOld(
      workflowIdPrefixO: Option[String]
  )(synchronizerId: SynchronizerId, contracts: Seq[RepairContract])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    for {
      alias <- EitherT.fromEither[Future](
        sync.aliasManager
          .aliasForSynchronizerId(synchronizerId)
          .toRight(s"Not able to find synchronizer alias for ${synchronizerId.toString}")
      )

      _ <- EitherT.fromEither[Future](
        sync.repairService.addContracts(
          alias,
          contracts,
          contractImportMode = ContractImportMode.Validation,
          packageMetadataSnapshot = sync.getPackageMetadataSnapshot,
          representativePackageIdOverride = RepresentativePackageIdOverride.NoOverride,
          workflowIdPrefix = workflowIdPrefixO,
        )
      )
    } yield ()

  override def migrateSynchronizer(
      request: MigrateSynchronizerRequest
  ): Future[MigrateSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // ensure here we don't process migration requests concurrently
    if (!synchronizerMigrationInProgress.getAndSet(true)) {
      val migratedSourceSynchronizer = for {
        sourceSynchronizerAlias <- EitherT.fromEither[Future](
          SynchronizerAlias.create(request.sourceSynchronizerAlias).map(Source(_))
        )
        conf <- EitherT
          .fromEither[Future](
            request.targetSynchronizerConnectionConfig
              .toRight("The target synchronizer connection configuration is required")
              .flatMap(
                SynchronizerConnectionConfig.fromProtoV30(_).leftMap(_.toString)
              )
              .map(Target(_))
          )
        _ <- EitherT(
          sync
            .migrateSynchronizer(sourceSynchronizerAlias, conf, force = request.force)
            .leftMap(_.asGrpcError.getStatus.getDescription)
            .value
            .onShutdown {
              Left("Aborted due to shutdown.")
            }
        )
      } yield MigrateSynchronizerResponse()

      EitherTUtil
        .toFuture(
          migratedSourceSynchronizer.leftMap(err =>
            io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException()
          )
        )
        .thereafter { _ =>
          synchronizerMigrationInProgress.set(false)
        }
    } else
      Future.failed(
        io.grpc.Status.ABORTED
          .withDescription(
            s"migrate_synchronizer for participant: ${sync.participantId} is already in progress"
          )
          .asRuntimeException()
      )
  }

  override def changeAssignation(
      request: ChangeAssignationRequest
  ): Future[ChangeAssignationResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    def synchronizerIdFromAlias(
        synchronizerAliasP: String
    ): EitherT[FutureUnlessShutdown, RepairServiceError, SynchronizerId] =
      for {
        alias <- EitherT
          .fromEither[FutureUnlessShutdown](SynchronizerAlias.create(synchronizerAliasP))
          .leftMap(err =>
            RepairServiceError.InvalidArgument
              .Error(s"Unable to parse $synchronizerAliasP as synchronizer alias: $err")
          )

        id <- EitherT.fromEither[FutureUnlessShutdown](
          sync.aliasManager
            .synchronizerIdForAlias(alias)
            .toRight[RepairServiceError](
              RepairServiceError.InvalidArgument
                .Error(s"Unable to find synchronizer id for alias $alias")
            )
        )
      } yield id

    val result = for {
      sourceSynchronizerId <- synchronizerIdFromAlias(request.sourceSynchronizerAlias).map(
        Source(_)
      )
      targetSynchronizerId <- synchronizerIdFromAlias(request.targetSynchronizerAlias).map(
        Target(_)
      )

      contracts <- EitherT.fromEither[FutureUnlessShutdown](request.contracts.traverse {
        case ChangeAssignationRequest.Contract(cidP, reassignmentCounterPO) =>
          val reassignmentCounter = reassignmentCounterPO.map(ReassignmentCounter(_))

          LfContractId
            .fromString(cidP)
            .leftMap[RepairServiceError](err =>
              RepairServiceError.InvalidArgument.Error(s"Unable to parse contract id `$cidP`: $err")
            )
            .map((_, reassignmentCounter))
      })

      _ <- NonEmpty.from(contracts) match {
        case None => EitherTUtil.unitUS[RepairServiceError]
        case Some(contractsNE) =>
          sync.repairService
            .changeAssignation(
              contracts = contractsNE,
              sourceSynchronizer = sourceSynchronizerId,
              targetSynchronizer = targetSynchronizerId,
              skipInactive = request.skipInactive,
            )
            .leftMap[RepairServiceError](RepairServiceError.ContractAssignationChangeError.Error(_))
      }
    } yield ()

    EitherTUtil
      .toFutureUnlessShutdown(
        result.bimap(
          _.asGrpcError,
          _ => ChangeAssignationResponse(),
        )
      )
      .asGrpcResponse

  }

  override def purgeDeactivatedSynchronizer(
      request: PurgeDeactivatedSynchronizerRequest
  ): Future[PurgeDeactivatedSynchronizerResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerAlias <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerAlias
          .fromProtoPrimitive(request.synchronizerAlias)
          .leftMap(_.toString)
          .leftMap(RepairServiceError.InvalidArgument.Error(_))
      )
      _ <- sync.purgeDeactivatedSynchronizer(synchronizerAlias).leftWiden[RpcError]
    } yield ()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.bimap(
          _.asGrpcError,
          _ => PurgeDeactivatedSynchronizerResponse(),
        )
      )
      .asGrpcResponse
  }

  override def ignoreEvents(request: IgnoreEventsRequest): Future[IgnoreEventsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        PhysicalSynchronizerId
          .fromProtoPrimitive(request.physicalSynchronizerId, "physical_synchronizer_id")
          .leftMap(_.message)
      )
      _ <- sync.repairService.ignoreEvents(
        synchronizerId,
        SequencerCounter(request.fromInclusive),
        SequencerCounter(request.toInclusive),
        force = request.force,
      )
    } yield IgnoreEventsResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def unignoreEvents(request: UnignoreEventsRequest): Future[UnignoreEventsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      synchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        PhysicalSynchronizerId
          .fromProtoPrimitive(request.physicalSynchronizerId, "physical_synchronizer_id")
          .leftMap(_.message)
      )
      _ <- sync.repairService.unignoreEvents(
        synchronizerId,
        SequencerCounter(request.fromInclusive),
        SequencerCounter(request.toInclusive),
        force = request.force,
      )
    } yield UnignoreEventsResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def rollbackUnassignment(
      request: RollbackUnassignmentRequest
  ): Future[RollbackUnassignmentResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      reassignmentId <- EitherT.fromEither[FutureUnlessShutdown](
        protocol.ReassignmentId
          .fromProtoPrimitive(request.reassignmentId)
          .leftMap(err => ValueConversionError("reassignment_id", err.message).message)
      )
      sourceSynchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(request.sourceSynchronizerId, "source")
          .map(Source(_))
          .leftMap(_.message)
      )
      targetSynchronizerId <- EitherT.fromEither[FutureUnlessShutdown](
        SynchronizerId
          .fromProtoPrimitive(request.targetSynchronizerId, "target")
          .map(Target(_))
          .leftMap(_.message)
      )

      _ <- sync.repairService.rollbackUnassignment(
        reassignmentId,
        sourceSynchronizerId,
        targetSynchronizerId,
      )

    } yield RollbackUnassignmentResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }

  override def repairCommitmentsUsingAcs(
      request: RepairCommitmentsUsingAcsRequest
  ): Future[RepairCommitmentsUsingAcsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val result = for {
      synchronizerIds <- wrapErrUS(
        request.synchronizerIds.traverse(SynchronizerId.fromProtoPrimitive(_, "synchronizer_id"))
      )
      counterParticipantsIds <- wrapErrUS(
        request.counterParticipantIds.traverse(
          ParticipantId.fromProtoPrimitive(_, "counter_participant_id")
        )
      )

      partyIds <- wrapErrUS(
        request.partyIds.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive(party, "party_ids").map(PartyId(_))
        )
      )

      timeoutSeconds <- wrapErrUS(
        ProtoConverter.parseRequired(
          NonNegativeFiniteDuration.fromProtoPrimitive("initialRetryDelay"),
          "timeoutSeconds",
          request.timeoutSeconds,
        )
      )

      res <- EitherT
        .right[RpcError](
          sync.commitmentsService
            .reinitializeCommitmentsUsingAcs(
              synchronizerIds.toSet,
              counterParticipantsIds,
              partyIds,
              timeoutSeconds,
            )
        )

    } yield v30.RepairCommitmentsUsingAcsResponse(res.map {
      case (synchronizerId: SynchronizerId, stringOrMaybeTimestamp) =>
        stringOrMaybeTimestamp match {
          case Left(err) =>
            v30.RepairCommitmentsStatus(
              synchronizerId.toProtoPrimitive,
              v30.RepairCommitmentsStatus.Status.ErrorMessage(err),
            )
          case Right(ts) =>
            v30.RepairCommitmentsStatus(
              synchronizerId.toProtoPrimitive,
              v30.RepairCommitmentsStatus.Status.CompletedRepairTimestamp(ts match {
                case Some(value) => value.toProtoTimestamp
                case None => CantonTimestamp.MinValue.toProtoTimestamp
              }),
            )
        }
    }.toSeq)

    CantonGrpcUtil.mapErrNewEUS(result)
  }

  override def performSynchronizerUpgrade(
      request: PerformSynchronizerUpgradeRequest
  ): Future[PerformSynchronizerUpgradeResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val res = for {
      validatedRequest <- CantonGrpcUtil.wrapErrUS(ManualLSURequest.fromProtoV30(request))
      _ <- sync
        .manuallyUpgradeSynchronizerTo(validatedRequest)
        .leftMap[RpcError](
          RepairServiceError.SynchronizerUpgradeError.Error(validatedRequest.successorPSId, _)
        )
    } yield PerformSynchronizerUpgradeResponse()

    CantonGrpcUtil.mapErrNewEUS(res)
  }
}

object GrpcParticipantRepairService {

  // TODO(#24610) - remove, used by ExportAcsOldRequest only
  @nowarn("cat=deprecation")
  private object ValidExportAcsOldRequest {
    private def validateRequestOld(
        request: ExportAcsOldRequest
    ): Either[String, ValidExportAcsOldRequest] =
      for {
        parties <- request.parties.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive_(party).map(PartyId(_).toLf).leftMap(_.message)
        )
        timestamp <- request.timestamp
          .traverse(CantonTimestamp.fromProtoTimestamp)
          .leftMap(_.message)
      } yield ValidExportAcsOldRequest(
        parties.toSet,
        timestamp,
        force = request.force,
        partiesOffboarding = request.partiesOffboarding,
      )

    def apply(
        request: ExportAcsOldRequest
    )(implicit
        elc: ErrorLoggingContext
    ): Either[RepairServiceError, ValidExportAcsOldRequest] =
      for {
        validRequest <- validateRequestOld(request).leftMap(
          RepairServiceError.InvalidArgument.Error(_)
        )
      } yield validRequest
  }

  // TODO(#24610) - remove, used by ExportAcsOldRequest only
  private final case class ValidExportAcsOldRequest private (
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      force: Boolean, // if true, does not check whether `timestamp` is clean
      partiesOffboarding: Boolean,
  )

  private final case class ValidExportAcsRequest(
      parties: Set[PartyId],
      atOffset: Offset,
      excludedStakeholders: Set[PartyId],
      synchronizerId: Option[SynchronizerId],
      contractSynchronizerRenames: Map[String, String],
  )

}
