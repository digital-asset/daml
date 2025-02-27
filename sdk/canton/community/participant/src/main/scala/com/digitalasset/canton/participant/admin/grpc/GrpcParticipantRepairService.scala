// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.ProtoDeserializationError.TimestampConversionError
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp.fromProtoPrimitive
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data
import com.digitalasset.canton.participant.admin.data.ActiveContract.loadFromByteString
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.ValidExportAcsRequest
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.admin.repair.{EnsureValidContractIds, RepairServiceError}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.synchronizer.SynchronizerConnectionConfig
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{PartyId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, GrpcStreamingUtils, MonadUtil, ResourceUtil}
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

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

final class GrpcParticipantRepairService(
    sync: CantonSyncService,
    parameters: ParticipantNodeParameters,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantRepairServiceGrpc.ParticipantRepairService
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

  /** originates from download above
    */
  override def exportAcs(
      request: ExportAcsRequest,
      responseObserver: StreamObserver[ExportAcsResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => createAcsSnapshotTemporaryFile(request, out),
      responseObserver,
      byteString => ExportAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )
  }

  private def createAcsSnapshotTemporaryFile(
      request: ExportAcsRequest,
      out: OutputStream,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val gzipOut = new GZIPOutputStream(out)
    val res = for {
      validRequest <- EitherT.fromEither[FutureUnlessShutdown](
        ValidExportAcsRequest(request, sync.stateInspection.allProtocolVersions)
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

    mapErrNewEUS(res.leftMap(_.toCantonError))
  }

  /** New endpoint to upload contracts for a party which uses the versioned ActiveContract
    */
  override def importAcs(
      responseObserver: StreamObserver[ImportAcsResponse]
  ): StreamObserver[ImportAcsRequest] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // TODO(i12481): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()
    // (workflowIdPrefix, allowContractIdSuffixRecomputation)
    val args = new AtomicReference[Option[(String, Boolean)]](None)
    def tryArgs: (String, Boolean) =
      args
        .get()
        .getOrElse(throw new IllegalStateException("The import ACS request fields are not set"))

    new StreamObserver[ImportAcsRequest] {
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

      override def onNext(request: ImportAcsRequest): Unit = {
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

      // TODO(i12481): implement a solution to prevent the client from sending infinite streams
      override def onCompleted(): Unit = {
        val (workflowIdPrefix, allowContractIdSuffixRecomputation) = tryArgs

        val res = importAcsSnapshot(
          data = ByteString.copyFrom(outputStream.toByteArray),
          workflowIdPrefix = workflowIdPrefix,
          allowContractIdSuffixRecomputation = allowContractIdSuffixRecomputation,
        )

        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(contractIdRemapping) =>
            responseObserver.onNext(ImportAcsResponse(contractIdRemapping))
            responseObserver.onCompleted()
        }
        outputStream.close()
      }
    }
  }

  private def importAcsSnapshot(
      data: ByteString,
      workflowIdPrefix: String,
      allowContractIdSuffixRecomputation: Boolean,
  )(implicit traceContext: TraceContext): Future[Map[String, String]] = {
    val resultET = for {
      activeContracts <- EitherT.fromEither[Future](
        loadFromByteString(data)
      )
      workflowIdPrefixO = Option.when(workflowIdPrefix != "")(workflowIdPrefix)

      activeContractsWithRemapping <-
        EnsureValidContractIds( // TODO(#22803) - Make this optional or 0 secs operation since it should be a no-op
          loggerFactory,
          sync.protocolVersionGetter,
          Option.when(allowContractIdSuffixRecomputation)(sync.pureCryptoApi),
        )(activeContracts)
      (activeContractsWithValidContractIds, contractIdRemapping) =
        activeContractsWithRemapping

      _ <- activeContractsWithValidContractIds.groupBy(_.synchronizerId).toSeq.parTraverse_ {
        case (synchronizerId, contracts) =>
          MonadUtil.batchedSequentialTraverse_(
            batching.parallelism,
            batching.maxAcsImportBatchSize,
          )(contracts)(writeContractsBatch(workflowIdPrefixO)(synchronizerId, _))
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

  private def writeContractsBatch(
      workflowIdPrefixO: Option[String]
  )(synchronizerId: SynchronizerId, contracts: Seq[data.ActiveContract])(implicit
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
          contracts.map(c => RepairContract(c.contract, c.reassignmentCounter)),
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
              batchSize = batching.maxItemsInBatch,
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
      _ <- sync.purgeDeactivatedSynchronizer(synchronizerAlias).leftWiden[CantonError]
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
        SynchronizerId
          .fromProtoPrimitive(request.synchronizerId, "synchronizer_id")
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
        SynchronizerId
          .fromProtoPrimitive(request.synchronizerId, "synchronizer_id")
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
      unassignId <- EitherT.fromEither[FutureUnlessShutdown](
        Try(request.unassignId.toLong).toEither.left
          .map(_ => TimestampConversionError(s"cannot convert ${request.unassignId} into Long"))
          .flatMap(fromProtoPrimitive)
          .leftMap(_.message)
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
      reassignmentId = protocol.ReassignmentId(sourceSynchronizerId, unassignId)

      _ <- sync.repairService.rollbackUnassignment(reassignmentId, targetSynchronizerId)

    } yield RollbackUnassignmentResponse()

    EitherTUtil
      .toFutureUnlessShutdown(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
      .asGrpcResponse
  }
}

object GrpcParticipantRepairService {

  private object ValidExportAcsRequest {

    private def validateContractSynchronizerRenames(
        contractSynchronizerRenames: Map[String, ExportAcsRequest.TargetSynchronizer],
        allProtocolVersions: Map[SynchronizerId, ProtocolVersion],
    ): Either[String, List[(SynchronizerId, (SynchronizerId, ProtocolVersion))]] =
      contractSynchronizerRenames.toList.traverse {
        case (
              source,
              ExportAcsRequest.TargetSynchronizer(targetSynchronizer, targetProtocolVersionRaw),
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

    private def validateRequest(
        request: ExportAcsRequest,
        allProtocolVersions: Map[SynchronizerId, ProtocolVersion],
    ): Either[String, ValidExportAcsRequest] =
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
      } yield ValidExportAcsRequest(
        parties.toSet,
        timestamp,
        contractSynchronizerRenames.toMap,
        force = request.force,
        partiesOffboarding = request.partiesOffboarding,
      )

    def apply(request: ExportAcsRequest, allProtocolVersions: Map[SynchronizerId, ProtocolVersion])(
        implicit elc: ErrorLoggingContext
    ): Either[RepairServiceError, ValidExportAcsRequest] =
      for {
        validRequest <- validateRequest(request, allProtocolVersions).leftMap(
          RepairServiceError.InvalidArgument.Error(_)
        )
      } yield validRequest

  }

  private final case class ValidExportAcsRequest private (
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      contractSynchronizerRenames: Map[SynchronizerId, (SynchronizerId, ProtocolVersion)],
      force: Boolean, // if true, does not check whether `timestamp` is clean
      partiesOffboarding: Boolean,
  )

}
