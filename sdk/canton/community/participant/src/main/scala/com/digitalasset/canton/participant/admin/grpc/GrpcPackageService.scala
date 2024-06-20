// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.error.ErrorCode
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.admin.*
import com.digitalasset.canton.participant.admin.v0.{DarDescription as ProtoDarDescription, *}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.canton.{LfPackageId, protocol}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcPackageService(
    service: PackageService,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends PackageServiceGrpc.PackageService
    with NamedLogging {

  override def listPackages(request: ListPackagesRequest): Future[ListPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      activePackages <- service.listPackages(OptionUtil.zeroAsNone(request.limit))
    } yield ListPackagesResponse(activePackages.map {
      case protocol.PackageDescription(pid, sourceDescription, _, _) =>
        v0.PackageDescription(pid, sourceDescription.unwrap)
    })
  }

  override def uploadDar(request: UploadDarRequest): Future[UploadDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      hash <- service.appendDarFromByteString(
        request.data,
        request.filename,
        request.vetAllPackages,
        request.synchronizeVetting,
      )
    } yield UploadDarResponse(
      UploadDarResponse.Value.Success(UploadDarResponse.Success(hash.toHexString))
    )
    EitherTUtil.toFuture(
      ret
        .leftMap(ErrorCode.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    )
  }

  override def validateDar(request: ValidateDarRequest): Future[ValidateDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret =
      service
        .validateDar(request.data, Some(request.filename))
        .map((hash: Hash) => ValidateDarResponse(hash.toHexString))
    EitherTUtil.toFuture(
      ret
        .leftMap(ErrorCode.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    )
  }

  override def removePackage(request: RemovePackageRequest): Future[RemovePackageResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val packageIdE: Either[StatusRuntimeException, LfPackageId] =
      LfPackageId
        .fromString(request.packageId)
        .left
        .map(_ =>
          Status.INVALID_ARGUMENT
            .withDescription(s"Invalid package ID: ${request.packageId}")
            .asRuntimeException()
        )

    val ret = {
      for {
        packageId <- EitherT.fromEither[Future](packageIdE)
        _unit <- service
          .removePackage(
            packageId,
            request.force,
          )
          .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error()))
          .leftMap(ErrorCode.asGrpcError)
      } yield {
        RemovePackageResponse(success = Some(Empty()))
      }
    }

    EitherTUtil.toFuture(ret)
  }

  override def vetDar(request: VetDarRequest): Future[VetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      hash <- EitherT.fromEither[Future](extractHash(request.darHash))
      _unit <- service
        .vetDar(hash, request.synchronize)
        .leftMap(_.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    } yield VetDarResponse()

    EitherTUtil.toFuture(ret)
  }

  override def unvetDar(request: UnvetDarRequest): Future[UnvetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val ret = for {
      hash <- EitherT.fromEither[Future](extractHash(request.darHash))
      _unit <- service
        .unvetDar(hash)
        .leftMap(_.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    } yield UnvetDarResponse()

    EitherTUtil.toFuture(ret)
  }

  override def removeDar(request: RemoveDarRequest): Future[RemoveDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val hashE = extractHash(request.darHash)
    val ret = {
      for {
        hash <- EitherT.fromEither[Future](hashE)
        _unit <- service
          .removeDar(hash)
          .leftMap(_.asGrpcError)
          .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
      } yield {
        RemoveDarResponse(success = Some(Empty()))
      }
    }

    EitherTUtil.toFuture(ret)
  }

  private def extractHash(apiHash: String): Either[StatusRuntimeException, Hash] =
    Hash
      .fromHexString(apiHash)
      .leftMap(err =>
        Status.INVALID_ARGUMENT
          .withDescription(s"Invalid dar hash: $apiHash [$err]")
          .asRuntimeException()
      )

  override def getDar(request: GetDarRequest): Future[GetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val darHash = Hash.tryFromHexString(request.hash)
    for {
      maybeDar <- service.getDar(darHash)
    } yield maybeDar.fold(GetDarResponse(data = ByteString.EMPTY, name = "")) { dar =>
      GetDarResponse(ByteString.copyFrom(dar.bytes), dar.descriptor.name.toProtoPrimitive)
    }
  }

  override def listDars(request: ListDarsRequest): Future[ListDarsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      dars <- service.listDars(OptionUtil.zeroAsNone(request.limit))
    } yield ListDarsResponse(dars.map { case DarDescriptor(hash, name) =>
      ProtoDarDescription(hash.toHexString, name.toProtoPrimitive)
    })
  }

  override def listDarContents(request: ListDarContentsRequest): Future[ListDarContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      hash <- EitherT.fromEither[Future](Hash.fromHexString(request.darId)).leftMap(_.toString)
      result <- service.listDarContents(hash)
    } yield {
      val (description, archive) = result
      ListDarContentsResponse(
        description = description.name.toProtoPrimitive,
        main = archive.main.getHash,
        packages = archive.all.map(_.getHash),
        dependencies = archive.dependencies.map(_.getHash),
      )
    }
    EitherTUtil.toFuture(res.leftMap(Status.NOT_FOUND.withDescription(_).asRuntimeException()))
  }

  override def listPackageContents(
      request: ListPackageContentsRequest
  ): Future[ListPackageContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    for {
      optModules <- service.getPackage(LfPackageId.assertFromString(request.packageId))
      modules = optModules.map(_.modules).getOrElse(Map.empty)
    } yield {
      ListPackageContentsResponse(modules.toSeq.map { case (moduleName, _) =>
        ModuleDescription(moduleName.dottedName)
      })
    }
  }

}
