// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.grpc

import cats.data.EitherT
import cats.implicits.catsSyntaxOptionId
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.error.ErrorCode
import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.ProtoDeserializationError.{
  ProtoDeserializationFailure,
  StringConversionError,
}
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.admin.participant.v30.{
  GetPackageReferencesRequest,
  GetPackageReferencesResponse,
}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarId}
import com.digitalasset.canton.participant.admin.{
  CantonPackageServiceError,
  PackageService,
  PackageVettingSynchronization,
}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, OptionUtil}
import com.digitalasset.daml.lf.data.Ref.ModuleName
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast.{DDataType, GenModule}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.{ExecutionContext, Future}

class GrpcPackageService(
    service: PackageService,
    synchronizeVetting: PackageVettingSynchronization,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends v30.PackageServiceGrpc.PackageService
    with NamedLogging {

  override def listPackages(request: v30.ListPackagesRequest): Future[v30.ListPackagesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ListPackagesRequest(limit, filterName) = request
    for {
      activePackages <- service
        .listPackages(OptionUtil.zeroAsNone(limit))
        .map(_.filter(_.name.str.startsWith(filterName)))
        .asGrpcFuture
    } yield v30.ListPackagesResponse(
      activePackages.map { case state.PackageDescription(pid, name, version, uploadedAt, size) =>
        v30.PackageDescription(
          packageId = pid,
          name = name.str,
          version = version.str,
          uploadedAt = Some(uploadedAt.toProtoTimestamp),
          size = size,
        )
      }
    )
  }

  override def validateDar(request: v30.ValidateDarRequest): Future[v30.ValidateDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret =
      service
        .validateDar(request.data, request.filename)
        .map(darId => v30.ValidateDarResponse(darId = darId.unwrap))
    EitherTUtil.toFuture(
      ret
        .leftMap(ErrorCode.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    )
  }

  override def uploadDar(request: v30.UploadDarRequest): Future[v30.UploadDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.UploadDarRequest(
      data,
      description,
      vetAllPackages,
      synchronizeVettingP,
      expectedMainPackageIdP,
    ) = request

    val ret =
      for {
        expectedMainPackageId <- Option
          .when(expectedMainPackageIdP.nonEmpty)(expectedMainPackageIdP)
          .traverse(parsePackageId)
          .leftMap(_.asGrpcError)
        darId <- service
          .upload(
            darBytes = data,
            description = Option.when(description.nonEmpty)(description),
            submissionIdO = None,
            vetAllPackages = vetAllPackages,
            synchronizeVetting =
              if (synchronizeVettingP) synchronizeVetting
              else PackageVettingSynchronization.NoSync,
            expectedMainPackageId,
          )
          .leftMap(_.asGrpcError)
      } yield v30.UploadDarResponse(darId = darId.unwrap)

    EitherTUtil.toFuture(
      ret
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    )
  }

  override def removePackage(
      request: v30.RemovePackageRequest
  ): Future[v30.RemovePackageResponse] = {
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

    val ret =
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
        v30.RemovePackageResponse(success = Some(Empty()))
      }

    EitherTUtil.toFuture(ret)
  }

  override def vetDar(request: v30.VetDarRequest): Future[v30.VetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      hash <- EitherT.fromEither[Future](extractDarId(request.darId))
      _unit <- service
        .vetDar(
          hash,
          if (request.synchronize) synchronizeVetting else PackageVettingSynchronization.NoSync,
        )
        .leftMap(_.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.VetDarResponse()

    EitherTUtil.toFuture(ret)
  }

  override def unvetDar(request: v30.UnvetDarRequest): Future[v30.UnvetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    val ret = for {
      hash <- EitherT.fromEither[Future](extractDarId(request.darId))
      _unit <- service
        .unvetDar(hash)
        .leftMap(_.asGrpcError)
        .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
    } yield v30.UnvetDarResponse()

    EitherTUtil.toFuture(ret)
  }

  override def removeDar(request: v30.RemoveDarRequest): Future[v30.RemoveDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val hashE = extractDarId(request.darId)
    val ret =
      for {
        hash <- EitherT.fromEither[Future](hashE)
        _unit <- service
          .removeDar(hash)
          .leftMap(_.asGrpcError)
          .onShutdown(Left(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError))
      } yield v30.RemoveDarResponse()

    EitherTUtil.toFuture(ret)
  }

  private def extractDarId(darId: String): Either[StatusRuntimeException, DarId] =
    DarId
      .create(darId)
      .leftMap(err =>
        Status.INVALID_ARGUMENT
          .withDescription(s"Invalid dar hash: $darId [$err]")
          .asRuntimeException()
      )

  override def getDar(request: v30.GetDarRequest): Future[v30.GetDarResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val ret = for {
      darId <- EitherT.fromEither[FutureUnlessShutdown](
        DarId
          .fromProtoPrimitive(request.darId)
          .leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      dar <- service
        .getDar(darId)
        .toRight(
          CantonPackageServiceError.Fetching.DarNotFound.Reject("getDar", darId.unwrap): CantonError
        )
    } yield v30.GetDarResponse(
      payload = ByteString.copyFrom(dar.bytes),
      data = darDescriptionToProto(dar.descriptor).some,
    )
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def listDars(request: v30.ListDarsRequest): Future[v30.ListDarsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.ListDarsRequest(limit, filterName) = request
    for {
      dars <- service
        .listDars(OptionUtil.zeroAsNone(limit))
        .map(_.filter(_.name.str.startsWith(filterName)))
        .asGrpcFuture
    } yield v30.ListDarsResponse(dars.map {
      case DarDescription(darId, description, name, version) =>
        v30.DarDescription(
          main = darId.unwrap,
          name = name.toProtoPrimitive,
          version = version.toProtoPrimitive,
          description = description.toProtoPrimitive,
        )
    })
  }

  // custom converters as description is defined in ledger-api, while admin api proto is only accessible in app
  private def packageDescriptionToProto(desc: state.PackageDescription): v30.PackageDescription = {
    val state.PackageDescription(packageId, name, version, uploadedAt, packageSize) = desc
    v30.PackageDescription(
      packageId = packageId,
      name = name.str,
      version = version.str,
      uploadedAt = uploadedAt.toProtoTimestamp.some,
      size = packageSize,
    )
  }
  private def darDescriptionToProto(descriptor: DarDescription): v30.DarDescription =
    v30
      .DarDescription(
        main = descriptor.darId.mainPackageId.toProtoPrimitive,
        name = descriptor.name.toProtoPrimitive,
        version = descriptor.version.toProtoPrimitive,
        description = descriptor.description.toProtoPrimitive,
      )

  override def getDarContents(
      request: v30.GetDarContentsRequest
  ): Future[v30.GetDarContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val res = for {
      darId <- EitherT.fromEither[FutureUnlessShutdown](
        DarId.fromProtoPrimitive(request.darId).leftMap(ProtoDeserializationFailure.Wrap(_))
      )
      description <- service
        .getDar(darId)
        .toRight(
          CantonPackageServiceError.Fetching.DarNotFound
            .Reject("getDar", darId.unwrap)
        )
      packages <- service
        .getDarContents(darId)
        .toRight(
          CantonPackageServiceError.Fetching.DarNotFound
            .Reject("getDarContents", darId.unwrap): CantonError
        )
    } yield {
      v30.GetDarContentsResponse(
        description = darDescriptionToProto(description.descriptor).some,
        packages = packages.map(packageDescriptionToProto),
      )
    }
    CantonGrpcUtil.mapErrNewEUS(res)
  }

  private def isUtilityPackage(modules: Map[ModuleName, GenModule[Ast.Expr]]): Boolean =
    // TODO(#17635) make the isUtilityPackage boolean flag in the lf code public
    modules.values.forall(mod =>
      mod.templates.isEmpty &&
        mod.interfaces.isEmpty &&
        mod.definitions.values.forall {
          case DDataType(serializable, _, _) => !serializable
          case _ => true
        }
    )

  private def parsePackageId(
      packageIdProto: String
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, CantonError, LfPackageId] =
    EitherT.fromEither[FutureUnlessShutdown](
      LfPackageId
        .fromString(packageIdProto)
        .leftMap(err =>
          ProtoDeserializationFailure.Wrap(StringConversionError(err, "packageId".some))
        )
    )
  override def getPackageContents(
      request: v30.GetPackageContentsRequest
  ): Future[v30.GetPackageContentsResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.GetPackageContentsRequest(packageIdProto) = request
    def notFound(pkg: LfPackageId): CantonError =
      CantonPackageServiceError.Fetching.InvalidPackageId.NotFound(pkg)
    val ret = for {
      packageId <- parsePackageId(packageIdProto)
      pkg <- EitherT(service.getPackage(packageId).map(_.toRight(notFound(packageId))))
      description <- service.getPackageDescription(packageId).toRight(notFound(packageId))
      modules = pkg.modules
    } yield {
      v30.GetPackageContentsResponse(
        description = packageDescriptionToProto(description).some,
        modules = modules.toSeq.map { case (moduleName, _) =>
          v30.ModuleDescription(name = moduleName.dottedName)
        },
        isUtilityPackage = isUtilityPackage(modules),
        languageVersion = pkg.languageVersion.toString,
      )
    }
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

  override def getPackageReferences(
      request: GetPackageReferencesRequest
  ): Future[GetPackageReferencesResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val v30.GetPackageReferencesRequest(packageIdProto) = request
    val ret = for {
      packageId <- parsePackageId(packageIdProto)
      dars <- EitherT.right[CantonError](service.getPackageReferences(packageId))
    } yield v30.GetPackageReferencesResponse(
      dars = dars.map(darDescriptionToProto)
    )
    CantonGrpcUtil.mapErrNewEUS(ret)
  }

}
