// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.{
  ValidExportAcsOldRequest,
  ValidExportAcsRequest,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.admin.repair.{
  ContractIdsImportProcessor,
  RepairServiceError,
}
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
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  LfPartyId,
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
  private def createAcsSnapshotTemporaryFile(
      request: ExportAcsOldRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val gzipOut = new GZIPOutputStream(out)
    val res = for {
      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        ValidExportAcsOldRequest(request, sync.stateInspection.allProtocolVersions)
      )
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
              validRequest.contractSynchronizerRenames,
              skipCleanTimestampCheck = validRequest.force,
              partiesOffboarding = validRequest.partiesOffboarding,
            )
        )
        .leftMap(RepairServiceError.fromAcsInspectionError(_, logger))
    } yield ()

    mapErrNewEUS(res.leftMap(_.toCantonRpcError))
  }

  override def importAcsOld(
      responseObserver: StreamObserver[ImportAcsOldResponse]
  ): StreamObserver[ImportAcsOldRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val outputStream = new ByteArrayOutputStream()
    val args = new AtomicReference[Option[(String, Boolean)]](None)
    def tryArgs: (String, Boolean) =
      args
        .get()
        .getOrElse(throw new IllegalStateException("The import ACS request fields are not set"))

    new StreamObserver[ImportAcsOldRequest] {
      def setOrCheck(
          workflowIdPrefix: String,
          allowContractIdSuffixRecomputation: Boolean,
      ): Try[Unit] =
        Try {
          val newOrMatchingValue = Some((workflowIdPrefix, allowContractIdSuffixRecomputation))
          if (!args.compareAndSet(None, newOrMatchingValue)) {
            val (oldWorkflowIdPrefix, oldAllowContractIdSuffixRecomputation) = tryArgs
            if (workflowIdPrefix != oldWorkflowIdPrefix) {
              throw new IllegalArgumentException(
                s"Workflow ID prefix cannot be changed from $oldWorkflowIdPrefix to $workflowIdPrefix"
              )
            } else if (
              oldAllowContractIdSuffixRecomputation != allowContractIdSuffixRecomputation
            ) {
              throw new IllegalArgumentException(
                s"Contract ID suffix recomputation cannot be changed from $oldAllowContractIdSuffixRecomputation to $allowContractIdSuffixRecomputation"
              )
            }
          }
        }

      override def onNext(request: ImportAcsOldRequest): Unit = {
        val processRequest =
          for {
            _ <- setOrCheck(request.workflowIdPrefix, request.allowContractIdSuffixRecomputation)
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
        val (workflowIdPrefix, allowContractIdSuffixRecomputation) = tryArgs

        val res = importAcsSnapshotOld(
          data = ByteString.copyFrom(outputStream.toByteArray),
          workflowIdPrefix = workflowIdPrefix,
          allowContractIdSuffixRecomputation = allowContractIdSuffixRecomputation,
        )

        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(contractIdRemapping) =>
            responseObserver.onNext(ImportAcsOldResponse(contractIdRemapping))
            responseObserver.onCompleted()
        }
        outputStream.close()
      }
    }
  }

  private def importAcsSnapshotOld(
      data: ByteString,
      workflowIdPrefix: String,
      allowContractIdSuffixRecomputation: Boolean,
  )(implicit traceContext: TraceContext): Future[Map[String, String]] =
    importAcsContractsOld(
      loadFromByteString(data).map(contracts => contracts.map(_.toRepairContract)),
      workflowIdPrefix,
      if (allowContractIdSuffixRecomputation) ContractImportMode.Recomputation
      else ContractImportMode.Validation,
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
              s"Contract ID import mode cannot be changed from $oldContractImportMode to $contractImportMode"
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

        val result: EitherT[Future, Throwable, Map[String, String]] = for {

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

          contractIdRemapping <- EitherT.liftF[Future, Throwable, Map[String, String]](
            ParticipantCommon.importAcsNewSnapshot(
              acsSnapshot = acsSnapshot,
              workflowIdPrefix = workflowIdPrefix,
              contractImportMode = contractImportMode,
              excludedStakeholders = excludedStakeholders,
              representativePackageIdOverride = representativePackageIdOverride,
              sync = sync,
              batching = batching,
              loggerFactory = loggerFactory,
            )
          )
        } yield contractIdRemapping

        result
          .thereafter { _ =>
            outputStream.close()
          }
          .value // Get the underlying Future[Either[...]]
          .onComplete {
            // The Future itself failed (e.g., a fatal error in `thereafter`)
            case Failure(exception) =>
              responseObserver.onError(exception)

            case Success(result) =>
              result match {
                case Left(exception) =>
                  responseObserver.onError(exception)
                case Right(contractIdRemapping) =>
                  responseObserver.onNext(ImportAcsResponse(contractIdRemapping))
                  responseObserver.onCompleted()
              }
          }
      }
    }
  }

  private def importAcsContractsOld(
      contracts: Either[String, List[RepairContract]],
      workflowIdPrefix: String,
      contractImportMode: ContractImportMode,
  )(implicit traceContext: TraceContext): Future[Map[String, String]] = {
    val resultET = for {
      repairContracts <- EitherT
        .fromEither[Future](contracts)
        .ensure( // TODO(#23073) - Remove this restriction once #27325 has been re-implemented
          "Found at least one contract with a non-zero reassignment counter. ACS import does not yet support it."
        )(_.forall(_.reassignmentCounter == ReassignmentCounter.Genesis))

      workflowIdPrefixO = Option.when(workflowIdPrefix != "")(workflowIdPrefix)

      activeContractsWithRemapping <-
        ContractIdsImportProcessor(
          loggerFactory,
          sync.syncPersistentStateManager,
          sync.pureCryptoApi,
          contractImportMode,
        )(repairContracts)
      (activeContractsWithValidContractIds, contractIdRemapping) =
        activeContractsWithRemapping

      _ <- activeContractsWithValidContractIds.groupBy(_.synchronizerId).toSeq.parTraverse_ {
        case (synchronizerId, contracts) =>
          MonadUtil.batchedSequentialTraverse_(
            batching.parallelism,
            batching.maxAcsImportBatchSize,
          )(contracts)(
            writeContractsBatchOld(workflowIdPrefixO)(synchronizerId, _)
          )
      }

    } yield contractIdRemapping

    resultET.value.flatMap {
      case Left(error) => Future.failed(ImportAcsError.Error(error).asGrpcError)
      case Right(contractIdRemapping) =>
        Future.successful(
          contractIdRemapping.map { case (oldCid, newCid) => (oldCid.coid, newCid.coid) }
        )
    }
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
          ignoreAlreadyAdded = true,
          ignoreStakeholderCheck = true,
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
}

object GrpcParticipantRepairService {

  // TODO(#24610) - remove, used by ExportAcsOldRequest only
  private object ValidExportAcsOldRequest {

    private def validateContractSynchronizerRenames(
        contractSynchronizerRenames: Map[String, ExportAcsOldRequest.TargetSynchronizer],
        allProtocolVersions: Map[SynchronizerId, ProtocolVersion],
    ): Either[String, List[(SynchronizerId, (SynchronizerId, ProtocolVersion))]] =
      contractSynchronizerRenames.toList.traverse {
        case (
              source,
              ExportAcsOldRequest.TargetSynchronizer(targetSynchronizer, targetProtocolVersionRaw),
            ) =>
          for {
            sourceId <- SynchronizerId
              .fromProtoPrimitive(source, "source synchronizer id")
              .leftMap(_.message)

            targetSynchronizerId <- SynchronizerId
              .fromProtoPrimitive(targetSynchronizer, "target synchronizer id")
              .leftMap(_.message)
            targetProtocolVersion <- ProtocolVersion
              .fromProtoPrimitive(targetProtocolVersionRaw)
              .leftMap(_.toString)

            /*
            The `targetProtocolVersion` should be the one running on the corresponding synchronizer.
             */
            _ <- allProtocolVersions
              .get(targetSynchronizerId)
              .map { foundProtocolVersion =>
                Either.cond(
                  foundProtocolVersion == targetProtocolVersion,
                  (),
                  s"Inconsistent protocol versions for synchronizer $targetSynchronizerId: found version is $foundProtocolVersion, passed is $targetProtocolVersion",
                )
              }
              .getOrElse(Either.unit)

          } yield (sourceId, (targetSynchronizerId, targetProtocolVersion))
      }

    private def validateRequestOld(
        request: ExportAcsOldRequest,
        allProtocolVersions: Map[SynchronizerId, ProtocolVersion],
    ): Either[String, ValidExportAcsOldRequest] =
      for {
        parties <- request.parties.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive_(party).map(PartyId(_).toLf).leftMap(_.message)
        )
        timestamp <- request.timestamp
          .traverse(CantonTimestamp.fromProtoTimestamp)
          .leftMap(_.message)
        contractSynchronizerRenames <- validateContractSynchronizerRenames(
          request.contractSynchronizerRenames,
          allProtocolVersions,
        )
      } yield ValidExportAcsOldRequest(
        parties.toSet,
        timestamp,
        contractSynchronizerRenames.toMap,
        force = request.force,
        partiesOffboarding = request.partiesOffboarding,
      )

    def apply(
        request: ExportAcsOldRequest,
        allProtocolVersions: Map[SynchronizerId, ProtocolVersion],
    )(implicit
        elc: ErrorLoggingContext
    ): Either[RepairServiceError, ValidExportAcsOldRequest] =
      for {
        validRequest <- validateRequestOld(request, allProtocolVersions).leftMap(
          RepairServiceError.InvalidArgument.Error(_)
        )
      } yield validRequest

  }

  // TODO(#24610) - remove, used by ExportAcsOldRequest only
  private final case class ValidExportAcsOldRequest private (
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      contractSynchronizerRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
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
