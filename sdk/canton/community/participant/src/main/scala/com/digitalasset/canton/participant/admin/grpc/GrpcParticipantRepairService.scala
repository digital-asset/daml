// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.all.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.participant.v30.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.admin.data.ActiveContract.loadFromByteString
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.ValidExportAcsRequest
import com.digitalasset.canton.participant.admin.inspection
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.admin.repair.{EnsureValidContractIds, RepairServiceError}
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{DomainId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, GrpcStreamingUtils, ResourceUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfPartyId, SequencerCounter}
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPOutputStream
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrpcParticipantRepairService {

  private val DefaultBatchSize = 1000

  private object ValidExportAcsRequest {

    private def validateContractDomainRenames(
        contractDomainRenames: Map[String, ExportAcsRequest.TargetDomain],
        allProtocolVersions: Map[DomainId, ProtocolVersion],
    ): Either[String, List[(DomainId, (DomainId, ProtocolVersion))]] =
      contractDomainRenames.toList.traverse {
        case (source, ExportAcsRequest.TargetDomain(targetDomain, targetProtocolVersionRaw)) =>
          for {
            sourceId <- DomainId.fromProtoPrimitive(source, "source domain id").leftMap(_.message)

            targetDomainId <- DomainId
              .fromProtoPrimitive(targetDomain, "target domain id")
              .leftMap(_.message)
            targetProtocolVersion <- ProtocolVersion
              .fromProtoPrimitive(targetProtocolVersionRaw)
              .leftMap(_.toString)

            /*
            The `targetProtocolVersion` should be the one running on the corresponding domain.
             */
            _ <- allProtocolVersions
              .get(targetDomainId)
              .map { foundProtocolVersion =>
                EitherUtil.condUnitE(
                  foundProtocolVersion == targetProtocolVersion,
                  s"Inconsistent protocol versions for domain $targetDomainId: found version is $foundProtocolVersion, passed is $targetProtocolVersion",
                )
              }
              .getOrElse(Right(()))

          } yield (sourceId, (targetDomainId, targetProtocolVersion))
      }

    private def validateRequest(
        request: ExportAcsRequest,
        allProtocolVersions: Map[DomainId, ProtocolVersion],
    ): Either[String, ValidExportAcsRequest] = {
      for {
        parties <- request.parties.traverse(party =>
          UniqueIdentifier.fromProtoPrimitive_(party).map(PartyId(_).toLf).leftMap(_.message)
        )
        timestamp <- request.timestamp
          .traverse(CantonTimestamp.fromProtoTimestamp)
          .leftMap(_.message)
        contractDomainRenames <- validateContractDomainRenames(
          request.contractDomainRenames,
          allProtocolVersions,
        )
      } yield ValidExportAcsRequest(
        parties.toSet,
        timestamp,
        contractDomainRenames.toMap,
        force = request.force,
        partiesOffboarding = request.partiesOffboarding,
      )
    }

    def apply(request: ExportAcsRequest, allProtocolVersions: Map[DomainId, ProtocolVersion])(
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
      contractDomainRenames: Map[DomainId, (DomainId, ProtocolVersion)],
      force: Boolean, // if true, does not check whether `timestamp` is clean
      partiesOffboarding: Boolean,
  )

}

final class GrpcParticipantRepairService(
    sync: CantonSyncService,
    processingTimeout: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantRepairServiceGrpc.ParticipantRepairService
    with NamedLogging {

  private val domainMigrationInProgress = new AtomicReference[Boolean](false)

  private def toRepairServiceError(
      error: inspection.Error
  )(implicit tc: TraceContext): RepairServiceError =
    error match {
      case inspection.Error.TimestampAfterPrehead(domainId, requested, clean) =>
        RepairServiceError.InvalidAcsSnapshotTimestamp.Error(
          requested,
          clean,
          domainId,
        )
      case inspection.Error.TimestampBeforePruning(domainId, requested, pruned) =>
        RepairServiceError.UnavailableAcsSnapshot.Error(
          requested,
          pruned,
          domainId,
        )
      case inspection.Error.InconsistentSnapshot(domainId, missingContract) =>
        logger.warn(
          s"Inconsistent ACS snapshot for domain $domainId. Contract $missingContract (and possibly others) is missing."
        )
        RepairServiceError.InconsistentAcsSnapshot.Error(domainId)
      case inspection.Error.SerializationIssue(domainId, contractId, errorMessage) =>
        logger.error(
          s"Contract $contractId for domain $domainId cannot be serialized due to: $errorMessage"
        )
        RepairServiceError.SerializationError.Error(domainId, contractId)
      case inspection.Error.InvariantIssue(domainId, contractId, errorMessage) =>
        logger.error(
          s"Contract $contractId for domain $domainId cannot be serialized due to an invariant violation: $errorMessage"
        )
        RepairServiceError.SerializationError.Error(domainId, contractId)
      case inspection.Error.OffboardingParty(domainId, error) =>
        RepairServiceError.InvalidArgument.Error(s"Parties offboarding on domain $domainId: $error")
    }

  /** purge contracts
    */
  override def purgeContracts(request: PurgeContractsRequest): Future[PurgeContractsResponse] = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val res: Either[RepairServiceError, Unit] = for {
        cids <- request.contractIds
          .traverse(LfContractId.fromString)
          .leftMap(RepairServiceError.InvalidArgument.Error(_))
        domain <- DomainAlias
          .fromProtoPrimitive(request.domain)
          .leftMap(_.toString)
          .leftMap(RepairServiceError.InvalidArgument.Error(_))

        cidsNE <- NonEmpty
          .from(cids)
          .toRight(RepairServiceError.InvalidArgument.Error("Missing contract ids to purge"))

        _ <- sync.repairService
          .purgeContracts(domain, cidsNE, request.ignoreAlreadyPurged)
          .leftMap(RepairServiceError.ContractPurgeError.Error(domain, _))
      } yield ()

      res.fold(
        err => Future.failed(err.asGrpcError),
        _ => Future.successful(PurgeContractsResponse()),
      )
    }
  }

  /** originates from download above
    */
  override def exportAcs(
      request: ExportAcsRequest,
      responseObserver: StreamObserver[ExportAcsResponse],
  ): Unit =
    GrpcStreamingUtils.streamToClient(
      (out: OutputStream) => createAcsSnapshotTemporaryFile(request, out),
      responseObserver,
      byteString => ExportAcsResponse(byteString),
      processingTimeout.unbounded.duration,
      chunkSizeO = None,
    )

  private def createAcsSnapshotTemporaryFile(
      request: ExportAcsRequest,
      out: OutputStream,
  ): Future[Unit] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val gzipOut = new GZIPOutputStream(out)
    val res = for {
      validRequest <- EitherT.fromEither[Future](
        ValidExportAcsRequest(request, sync.stateInspection.allProtocolVersions)
      )
      timestampAsString = validRequest.timestamp.fold("head")(ts => s"at $ts")
      _ = logger.info(
        s"Exporting active contract set ($timestampAsString) for parties ${validRequest.parties}"
      )
      _ <- ResourceUtil
        .withResourceEitherT(gzipOut)(
          sync.stateInspection
            .exportAcsDumpActiveContracts(
              _,
              _.filterString.startsWith(request.filterDomainId),
              validRequest.parties,
              validRequest.timestamp,
              validRequest.contractDomainRenames,
              skipCleanTimestampCheck = validRequest.force,
              partiesOffboarding = validRequest.partiesOffboarding,
            )
        )
        .leftMap(toRepairServiceError)
    } yield ()

    mapErrNew(res)
  }

  /** New endpoint to upload contracts for a party which uses the versioned ActiveContract
    */
  override def importAcs(
      responseObserver: StreamObserver[ImportAcsResponse]
  ): StreamObserver[ImportAcsRequest] = {

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
        val res = TraceContext.withNewTraceContext { implicit traceContext =>
          val resultE = for {
            activeContracts <- EitherT.fromEither[Future](
              loadFromByteString(ByteString.copyFrom(outputStream.toByteArray))
            )
            (workflowIdPrefix, allowContractIdSuffixRecomputation) = tryArgs
            activeContractsWithRemapping <- EnsureValidContractIds(
              loggerFactory,
              sync.protocolVersionGetter,
              Option.when(allowContractIdSuffixRecomputation)(sync.pureCryptoApi),
            )(activeContracts)
            (activeContractsWithValidContractIds, contractIdRemapping) =
              activeContractsWithRemapping
            contractsByDomain = activeContractsWithValidContractIds
              .grouped(GrpcParticipantRepairService.DefaultBatchSize)
              .map(_.groupBy(_.domainId)) // TODO(#14822): group by domain first, and then batch
            _ <- LazyList
              .from(contractsByDomain)
              .parTraverse(_.toList.parTraverse {
                case (
                      domainId,
                      contracts,
                    ) => // TODO(#12481): large number of groups = large number of requests
                  for {
                    alias <- EitherT.fromEither[Future](
                      sync.aliasManager
                        .aliasForDomainId(domainId)
                        .toRight(s"Not able to find domain alias for ${domainId.toString}")
                    )
                    _ <- EitherT.fromEither[Future](
                      sync.repairService.addContracts(
                        alias,
                        contracts.map(c =>
                          RepairContract(
                            c.contract,
                            Set.empty,
                            c.transferCounter,
                          )
                        ),
                        ignoreAlreadyAdded = true,
                        ignoreStakeholderCheck = true,
                        workflowIdPrefix = Option.when(workflowIdPrefix != "")(workflowIdPrefix),
                      )
                    )
                  } yield ()
              })
          } yield contractIdRemapping

          resultE.value.flatMap {
            case Left(error) => Future.failed(ImportAcsError.Error(error).asGrpcError)
            case Right(contractIdRemapping) =>
              Future.successful(
                contractIdRemapping.map { case (oldCid, newCid) => (oldCid.coid, newCid.coid) }
              )
          }
        }

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

  override def migrateDomain(request: MigrateDomainRequest): Future[MigrateDomainResponse] = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      // ensure here we don't process migration requests concurrently
      if (!domainMigrationInProgress.getAndSet(true)) {
        val migratedSourceDomain = for {
          sourceDomainAlias <- EitherT.fromEither[Future](DomainAlias.create(request.sourceAlias))
          conf <- EitherT
            .fromEither[Future](
              request.targetDomainConnectionConfig
                .toRight("The target domain connection configuration is required")
                .flatMap(
                  DomainConnectionConfig.fromProtoV30(_).leftMap(_.toString)
                )
            )
          _ <- EitherT(
            sync
              .migrateDomain(sourceDomainAlias, conf, force = request.force)
              .leftMap(_.asGrpcError.getStatus.getDescription)
              .value
              .onShutdown {
                Left("Aborted due to shutdown.")
              }
          )
        } yield MigrateDomainResponse()

        EitherTUtil
          .toFuture(
            migratedSourceDomain.leftMap(err =>
              io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException()
            )
          )
          .andThen { _ =>
            domainMigrationInProgress.set(false)
          }
      } else
        Future.failed(
          io.grpc.Status.ABORTED
            .withDescription(
              s"migrate_domain for participant: ${sync.participantId} is already in progress"
            )
            .asRuntimeException()
        )
    }
  }

  /* Purge specified deactivated sync-domain and selectively prune domain stores.
   */
  override def purgeDeactivatedDomain(
      request: PurgeDeactivatedDomainRequest
  ): Future[PurgeDeactivatedDomainResponse] = TraceContext.withNewTraceContext {
    implicit traceContext =>
      val res = for {
        domainAlias <- EitherT.fromEither[FutureUnlessShutdown](
          DomainAlias
            .fromProtoPrimitive(request.domainAlias)
            .leftMap(_.toString)
            .leftMap(RepairServiceError.InvalidArgument.Error(_))
        )
        _ <- sync.purgeDeactivatedDomain(domainAlias)

      } yield ()

      EitherTUtil
        .toFutureUnlessShutdown(
          res.bimap(
            _.asGrpcError,
            _ => PurgeDeactivatedDomainResponse(),
          )
        )
        .asGrpcResponse
  }

  override def ignoreEvents(request: IgnoreEventsRequest): Future[IgnoreEventsResponse] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val res = for {
        domainId <- EitherT.fromEither[Future](
          DomainId.fromProtoPrimitive(request.domainId, "domain_id").leftMap(_.message)
        )
        _ <- sync.repairService.ignoreEvents(
          domainId,
          SequencerCounter(request.fromInclusive),
          SequencerCounter(request.toInclusive),
          force = request.force,
        )
      } yield IgnoreEventsResponse()

      EitherTUtil.toFuture(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
    }

  override def unignoreEvents(request: UnignoreEventsRequest): Future[UnignoreEventsResponse] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      val res = for {
        domainId <- EitherT.fromEither[Future](
          DomainId.fromProtoPrimitive(request.domainId, "domain_id").leftMap(_.message)
        )
        _ <- sync.repairService.unignoreEvents(
          domainId,
          SequencerCounter(request.fromInclusive),
          SequencerCounter(request.toInclusive),
          force = request.force,
        )
      } yield UnignoreEventsResponse()

      EitherTUtil.toFuture(
        res.leftMap(err => io.grpc.Status.CANCELLED.withDescription(err).asRuntimeException())
      )
    }
}
