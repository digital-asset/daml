// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.crypto.store.CryptoPublicStore
import com.digitalasset.canton.environment.CantonNodeBootstrapBase
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.admin.v0.*
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.empty.Empty
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status

import scala.concurrent.{ExecutionContext, Future}

class GrpcInitializationService(
    clock: Clock,
    bootstrap: CantonNodeBootstrapBase[_, _, _, _],
    cryptoPublicStore: CryptoPublicStore,
)(implicit ec: ExecutionContext)
    extends InitializationServiceGrpc.InitializationService {

  override def initId(request: InitIdRequest): Future[InitIdResponse] = {
    for {
      fp <- Fingerprint
        .fromProtoPrimitive(request.fingerprint)
        .toEitherT
        .valueOr(err => throw CantonGrpcUtil.invalidArgument(err.toString))
      id <- Identifier
        .fromProtoPrimitive(request.identifier)
        .toEitherT
        .valueOr(err => throw CantonGrpcUtil.invalidArgument(err.toString))
      uid = UniqueIdentifier(id, Namespace(fp))
      maybeKey <- cryptoPublicStore.signingKey(fp)(TraceContext.empty).value
      result <- maybeKey match {
        case Left(storeError) =>
          Future.failed(
            Status.INTERNAL
              .withDescription(s"Error reading public key [$fp] from database $storeError")
              .asException()
          )
        case Right(None) =>
          Future.failed(
            Status.INVALID_ARGUMENT
              .withDescription(s"Unknown public key [$fp], you need to import it first.")
              .asException()
          )
        case Right(Some(_)) =>
          bootstrap.initializeWithProvidedId(NodeId(uid)).value.flatMap {
            case Left(err) =>
              Future.failed(Status.ALREADY_EXISTS.withDescription(err).asException())
            case Right(()) =>
              Future.successful(
                InitIdResponse(uniqueIdentifier = uid.toProtoPrimitive, instance = "")
              )
          }
      }
    } yield result
  }

  override def getId(request: Empty): Future[GetIdResponse] = {
    val id = bootstrap.getId
    Future.successful(
      GetIdResponse(
        initialized = bootstrap.isInitialized,
        uniqueIdentifier = id.map(_.identity.toProtoPrimitive).getOrElse(""),
        instance = bootstrap.name.unwrap,
      )
    )
  }

  override def currentTime(request: Empty): Future[Timestamp] =
    Future.successful(clock.now.toProtoPrimitive)

}
