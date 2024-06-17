// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import better.files.*
import cats.data.EitherT
import cats.syntax.all.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.participant.admin.data.ActiveContract.loadFromByteString
import com.digitalasset.canton.participant.admin.data.SerializableContractWithDomainId
import com.digitalasset.canton.participant.admin.grpc.GrpcParticipantRepairService.{
  DefaultChunkSize,
  ValidDownloadRequest,
  ValidExportAcsRequest,
}
import com.digitalasset.canton.participant.admin.inspection
import com.digitalasset.canton.participant.admin.repair.RepairServiceError
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.admin.v0.*
import com.digitalasset.canton.participant.domain.DomainConnectionConfig
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.protocol.{LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{DomainId, PartyId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, EitherUtil, OptionUtil, ResourceUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, LfPartyId, SequencerCounter}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.StreamObserver

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object GrpcParticipantRepairService {

  // 2MB - This is half of the default max message size of gRPC
  val DefaultChunkSize: PositiveInt =
    PositiveInt.tryCreate(1024 * 1024 * 2)

  private val DefaultBatchSize = 1000

  // TODO(i14441): Remove deprecated ACS download / upload functionality
  @deprecated("Use ValidExportAcsRequest", since = "2.8.0")
  private object ValidDownloadRequest {

    // 2MB - This is half of the default max message size of gRPC
    val DefaultChunkSize: PositiveInt =
      PositiveInt.tryCreate(1024 * 1024 * 2)

    private val DefaultBatchSize = 1000

    private def validateParties(parties: Seq[String]): Either[String, Seq[LfPartyId]] =
      parties
        .map(party => UniqueIdentifier.fromProtoPrimitive_(party).map(PartyId(_).toLf))
        .sequence

    private def validateTimestamp(
        timestamp: Option[Timestamp]
    ): Either[String, Option[CantonTimestamp]] =
      timestamp.map(CantonTimestamp.fromProtoPrimitive).sequence.leftMap(_.message)

    private def validateProtocolVersion(
        protocolVersion: String
    ): Either[String, Option[ProtocolVersion]] =
      OptionUtil.emptyStringAsNone(protocolVersion).traverse(ProtocolVersion.create(_))

    private def validateContractDomainRenames(
        contractDomainRenames: Map[String, String]
    ): Either[String, List[(DomainId, DomainId)]] =
      contractDomainRenames.toList.traverse { case (source, target) =>
        for {
          sourceId <- DomainId.fromString(source)
          targetId <- DomainId.fromString(target)
        } yield (sourceId, targetId)
      }

    private def validateChunkSize(chunkSize: Option[Int]): Either[String, PositiveInt] =
      chunkSize.traverse(PositiveInt.create).bimap(_.message, _.getOrElse(DefaultChunkSize))

    private def validateAll(request: DownloadRequest): Either[String, ValidDownloadRequest] =
      for {
        parties <- validateParties(request.parties)
        timestamp <- validateTimestamp(request.timestamp)
        protocolVersion <- validateProtocolVersion(request.protocolVersion)
        contractDomainRenames <- validateContractDomainRenames(request.contractDomainRenames)
        chunkSize <- validateChunkSize(request.chunkSize)
      } yield ValidDownloadRequest(
        parties.toSet,
        timestamp,
        protocolVersion,
        contractDomainRenames.toMap,
        chunkSize,
      )

    def apply(request: DownloadRequest)(implicit
        elc: ErrorLoggingContext
    ): Either[RepairServiceError, ValidDownloadRequest] =
      for {
        validRequest <- validateAll(request).leftMap(RepairServiceError.InvalidArgument.Error(_))
        _ <- validRequest.protocolVersion.traverse_(isSupported)
      } yield validRequest

  }

  // TODO(i14441): Remove deprecated ACS download / upload functionality
  @deprecated("Use ValidExportAcsRequest", since = "2.8.0")
  private final case class ValidDownloadRequest private (
      parties: Set[LfPartyId],
      timestamp: Option[CantonTimestamp],
      protocolVersion: Option[ProtocolVersion],
      contractDomainRenames: Map[DomainId, DomainId],
      chunkSize: PositiveInt,
  )

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
          UniqueIdentifier.fromProtoPrimitive_(party).map(PartyId(_).toLf)
        )
        timestamp <- request.timestamp
          .traverse(CantonTimestamp.fromProtoPrimitive)
          .leftMap(_.message)
        contractDomainRenames <- validateContractDomainRenames(
          request.contractDomainRenames,
          allProtocolVersions,
        )
      } yield ValidExportAcsRequest(
        parties.toSet,
        timestamp,
        contractDomainRenames.toMap,
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
      partiesOffboarding: Boolean,
  )

  private def isSupported(protocolVersion: ProtocolVersion)(implicit
      elc: ErrorLoggingContext
  ): Either[RepairServiceError, Unit] =
    EitherUtil.condUnitE(
      protocolVersion.isSupported,
      RepairServiceError.UnsupportedProtocolVersionParticipant.Error(protocolVersion),
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

  private final val AcsSnapshotTemporaryFileNamePrefix = "temporary-canton-acs-snapshot"

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

        offboardedParties <- request.offboardedParties
          .traverse(LfPartyId.fromString)
          .leftMap(err =>
            RepairServiceError.InvalidArgument
              .Error(s"Unable to parse party from `offboarded_parties`: $err")
          )

        _ <- sync.repairService
          .purgeContracts(
            domain,
            cidsNE,
            NonEmpty.from(offboardedParties.toSet),
            request.ignoreAlreadyPurged,
          )
          .leftMap(RepairServiceError.ContractPurgeError.Error(domain, _))
      } yield ()

      res.fold(
        err => Future.failed(err.asGrpcError),
        _ => Future.successful(PurgeContractsResponse()),
      )
    }
  }

  /** get contracts for a party
    */
  @deprecated(
    "Use exportAcs",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  override def download(
      request: DownloadRequest,
      responseObserver: StreamObserver[AcsSnapshotChunk],
  ): Unit = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val (temporaryFile, outputStream) =
        if (request.gzipFormat) {
          val file = File.newTemporaryFile(AcsSnapshotTemporaryFileNamePrefix, suffix = ".gz")
          file -> file.newGzipOutputStream()
        } else {
          val file = File.newTemporaryFile(AcsSnapshotTemporaryFileNamePrefix)
          file -> file.newOutputStream()
        }

      def createAcsSnapshotTemporaryFile(
          validRequest: ValidDownloadRequest
      ): EitherT[Future, RepairServiceError, Unit] = {
        val timestampAsString = validRequest.timestamp.fold("head")(ts => s"at $ts")
        logger.info(
          s"Downloading active contract set ($timestampAsString) to $temporaryFile for parties ${validRequest.parties}"
        )
        ResourceUtil
          .withResourceEitherT(outputStream)(
            sync.stateInspection
              .dumpActiveContracts(
                _,
                _.filterString.startsWith(request.filterDomainId),
                validRequest.parties,
                validRequest.timestamp,
                validRequest.protocolVersion,
                validRequest.contractDomainRenames,
              )
          )
          .leftMap(toRepairServiceError)
      }

      // Create a context that will be automatically cancelled after the processing timeout deadline
      val context = io.grpc.Context.current().withCancellation()

      context.run { () =>
        val result =
          for {
            validRequest <- EitherT.fromEither[Future](ValidDownloadRequest(request))
            _ <- createAcsSnapshotTemporaryFile(validRequest)
          } yield {
            temporaryFile.newInputStream
              .buffered(validRequest.chunkSize.value)
              .autoClosed { s =>
                Iterator
                  .continually(s.readNBytes(validRequest.chunkSize.value))
                  .takeWhile(_.nonEmpty && !context.isCancelled)
                  .foreach { chunk =>
                    responseObserver.onNext(AcsSnapshotChunk(ByteString.copyFrom(chunk)))
                  }
              }
          }

        Await
          .result(result.value, processingTimeout.unbounded.duration) match {
          case Right(_) =>
            if (!context.isCancelled) responseObserver.onCompleted()
            else {
              context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
              ()
            }
          case Left(error) =>
            responseObserver.onError(error.asGrpcError)
            context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
            ()
        }
        // clean the temporary file used to store the acs
        temporaryFile.delete()
      }
    }
  }

  private final val ExportAcsTemporaryFileNamePrefix = "temporary-canton-acs-snapshot-versioned"

  /** originates from download above
    */
  override def exportAcs(
      request: ExportAcsRequest,
      responseObserver: StreamObserver[ExportAcsResponse],
  ): Unit = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val temporaryFile = File.newTemporaryFile(ExportAcsTemporaryFileNamePrefix, suffix = ".gz")
      val outputStream = temporaryFile.newGzipOutputStream()

      def createAcsSnapshotTemporaryFile(
          validRequest: ValidExportAcsRequest
      ): EitherT[Future, RepairServiceError, Unit] = {
        val timestampAsString = validRequest.timestamp.fold("head")(ts => s"at $ts")
        logger.info(
          s"Exporting active contract set ($timestampAsString) to $temporaryFile for parties ${validRequest.parties}"
        )
        ResourceUtil
          .withResourceEitherT(outputStream)(
            sync.stateInspection
              .exportAcsDumpActiveContracts(
                _,
                _.filterString.startsWith(request.filterDomainId),
                validRequest.parties,
                validRequest.timestamp,
                validRequest.contractDomainRenames,
                partiesOffboarding = validRequest.partiesOffboarding,
              )
          )
          .leftMap(toRepairServiceError)
      }

      // Create a context that will be automatically cancelled after the processing timeout deadline
      val context = io.grpc.Context.current().withCancellation()

      context.run { () =>
        val result =
          for {
            validRequest <- EitherT.fromEither[Future](
              ValidExportAcsRequest(request, sync.stateInspection.allProtocolVersions)
            )
            _ <- createAcsSnapshotTemporaryFile(validRequest)
          } yield {
            temporaryFile.newInputStream
              .buffered(DefaultChunkSize.value)
              .autoClosed { s =>
                Iterator
                  .continually(s.readNBytes(DefaultChunkSize.value))
                  .takeWhile(_.nonEmpty && !context.isCancelled)
                  .foreach { chunk =>
                    responseObserver.onNext(
                      ExportAcsResponse(
                        ByteString.copyFrom(chunk)
                      )
                    )
                  }
              }
          }

        Await
          .result(result.value, processingTimeout.unbounded.duration) match {
          case Right(_) =>
            if (!context.isCancelled) responseObserver.onCompleted()
            else {
              context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
              ()
            }
          case Left(error) =>
            responseObserver.onError(error.asGrpcError)
            context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
            ()
        }
        // clean the temporary file used to store the acs
        temporaryFile.delete()
      }
    }
  }

  /** upload contracts for a party
    */
  @deprecated(
    "Use importAcs",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  override def upload(
      responseObserver: StreamObserver[UploadResponse]
  ): StreamObserver[UploadRequest] = {
    // TODO(i12481): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()
    val gzip = new AtomicBoolean(false)

    new StreamObserver[UploadRequest] {
      override def onNext(value: UploadRequest): Unit = {
        if (value.gzipFormat && !gzip.get())
          gzip.set(true)
        Try(outputStream.write(value.acsSnapshot.toByteArray)) match {
          case Failure(exception) =>
            outputStream.close()
            responseObserver.onError(exception)
          case Success(_) =>
        }
      }

      override def onError(t: Throwable): Unit = {
        responseObserver.onError(t)
        outputStream.close()
      }

      // TODO(i12481): implement a solution to prevent the client from sending infinite streams
      override def onCompleted(): Unit = {
        val res =
          convertAndAddContractsToStore(ByteString.copyFrom(outputStream.toByteArray), gzip.get())
        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(_) =>
            responseObserver.onNext(UploadResponse())
            responseObserver.onCompleted()
        }
        outputStream.close()
      }
    }
  }

  /** New endpoint to upload contracts for a party which uses the versioned ActiveContract
    */
  override def importAcs(
      responseObserver: StreamObserver[ImportAcsResponse]
  ): StreamObserver[ImportAcsRequest] = {
    // TODO(i12481): This buffer will contain the whole ACS snapshot.
    val outputStream = new ByteArrayOutputStream()

    val workflowIdPrefix = new AtomicReference[Option[String]](None)
    val hostedParties = new AtomicReference[Option[Set[LfPartyId]]](None)

    /* Update the atomic reference with the new value if there is not conflict
     * return An error if the parsed value differs from the previously read value
     */
    def updateValue[T](existing: AtomicReference[Option[T]], parsed: T): Either[String, Unit] = {
      val oldValueO = existing.getAndSet(Some(parsed))

      oldValueO match {
        case Some(oldValue) =>
          Either.cond(oldValue == parsed, (), s"Cannot override `$oldValue` with parsed `$parsed`")
        case None => Right(())
      }
    }

    new StreamObserver[ImportAcsRequest] {
      override def onNext(importAcsRequest: ImportAcsRequest): Unit = {
        Try(outputStream.write(importAcsRequest.acsSnapshot.toByteArray)) match {
          case Failure(exception) =>
            outputStream.close()
            responseObserver.onError(exception)

          case Success(_) =>
            updateValue(workflowIdPrefix, importAcsRequest.workflowIdPrefix).left.foreach { err =>
              outputStream.close()
              responseObserver.onError(
                new IllegalArgumentException(
                  s"Unable to update from `workflowIdPrefix`: $err"
                )
              )
            }

            val res = for {
              _ <- updateValue(workflowIdPrefix, importAcsRequest.workflowIdPrefix).leftMap(err =>
                s"Unable to update from `workflow_id_prefix`: $err"
              )

              parsedOnboardedParties <- importAcsRequest.onboardedParties
                .traverse(LfPartyId.fromString)
                .leftMap(err => s"Unable to parse hosted_parties: $err")

              _ <- updateValue(hostedParties, parsedOnboardedParties.toSet).leftMap(err =>
                s"Unable to update from `hosted_parties`: $err"
              )
            } yield ()

            res.left.foreach { error =>
              outputStream.close()
              responseObserver.onError(new IllegalArgumentException(error))
            }
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
            contractsByDomain = activeContracts
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
                          )
                        ),
                        ignoreAlreadyAdded = true,
                        ignoreStakeholderCheck = true,
                        hostedParties = hostedParties.get().flatMap(NonEmpty.from),
                        workflowIdPrefix = workflowIdPrefix.get(),
                      )
                    )
                  } yield ()
              })
          } yield ()

          resultE.value.flatMap {
            case Left(error) => Future.failed(ImportAcsError.Error(error).asGrpcError)
            case Right(_) => Future.successful(UploadResponse())
          }
        }

        Try(Await.result(res, processingTimeout.unbounded.duration)) match {
          case Failure(exception) => responseObserver.onError(exception)
          case Success(_) =>
            responseObserver.onNext(ImportAcsResponse())
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
                  DomainConnectionConfig.fromProtoV0(_).leftMap(_.toString)
                )
            )
          _ <- EitherT(
            sync
              .migrateDomain(sourceDomainAlias, conf, force = request.force.getOrElse(false))
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

  @deprecated(
    "Use importAcs functionality instead",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  private def convertAndAddContractsToStore(
      content: ByteString,
      gzip: Boolean,
  ): Future[UploadResponse] = {
    TraceContext.withNewTraceContext { implicit traceContext =>
      val resultE: EitherT[Future, String, Unit] = for {
        lazyContracts <- EitherT.fromEither[Future](
          SerializableContractWithDomainId.loadFromByteString(content, gzip)
        )
        grouped = lazyContracts
          .grouped(GrpcParticipantRepairService.DefaultBatchSize)
          .map(
            _.groupMap(_.domainId)(_.contract)
          ) // TODO(#14822): group by domain first, and then batch
        _ <- LazyList
          .from(grouped)
          .parTraverse(_.toList.parTraverse {
            case (
                  domainId,
                  contracts,
                ) => // TODO(#12481): large number of groups = large number of requests
              addContractToStore(domainId, contracts)
          })
      } yield ()

      resultE.value.flatMap {
        case Left(error) => Future.failed(ImportAcsError.Error(error).asGrpcError)
        case Right(_) => Future.successful(UploadResponse())
      }

    }
  }

  @deprecated(
    "Use importAcs functionality instead",
    since = "2.8.0",
  ) // TODO(i14441): Remove deprecated ACS download / upload functionality
  private def addContractToStore(domainId: DomainId, contracts: LazyList[SerializableContract])(
      implicit traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {
    for {
      alias <- EitherT.fromEither[Future](
        sync.aliasManager
          .aliasForDomainId(domainId)
          .toRight(s"Not able to find domain alias for ${domainId.toString}")
      )
      _ <- EitherT.fromEither[Future](
        sync.repairService.addContracts(
          alias,
          contracts.map(contract => RepairContract(contract, Set.empty)),
          hostedParties = None,
          ignoreAlreadyAdded = true,
          ignoreStakeholderCheck = true,
        )
      )
    } yield ()
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
