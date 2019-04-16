// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.server.api.validation

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.lfpackage.Ast.Package
import com.digitalasset.grpc.adapter.utils.DirectExecutionContext
import com.digitalasset.ledger.api.v1.value.Identifier
import com.digitalasset.ledger.api.validation.IdentifierValidator
import com.github.benmanes.caffeine.cache.CaffeineSpec
import com.github.blemale.scaffeine.Scaffeine
import io.grpc.{Status, StatusRuntimeException}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

trait IdentifierResolverLike {
  def resolveIdentifier(identifier: Identifier): Either[StatusRuntimeException, Ref.Identifier]
}

class IdentifierResolver(packageResolver: Ref.PackageId => Future[Option[Package]])
    extends ErrorFactories
    with IdentifierResolverLike {

  /**
    * We could make this configurable at runtime, but it's so deep in the implementation
    * that it would be confusing for a user.
    */
  private val cacheSpec = "maximumSize=256"

  /**
    * Only the results of successful lookups are stored.
    * The runtime will continue to pay full price for failures.
    */
  private val idCache = buildCache()

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def buildCache() = {
    Scaffeine(CaffeineSpec.parse(cacheSpec))
      .buildAsyncFuture[Identifier, Ref.Identifier](resolveIdentifierAsync)(DirectExecutionContext)
  }

  def resolveIdentifier(identifier: Identifier): Either[StatusRuntimeException, Ref.Identifier] = {
    try {
      Right(Await.result(idCache.get(identifier), 5.seconds))
    } catch {
      case NonFatal(sre: StatusRuntimeException) => Left(sre)
      case NonFatal(_) => Left(generalGrpcError(identifier))
    }
  }
  private def generalGrpcError(identifier: Identifier) = {
    grpcError(
      Status.ABORTED.withDescription(s"Failed to resolve identifier named $identifier in time. " +
        "Please use module_name and entity_name fields to uniquely identify DAML entities."))
  }
  private def resolveIdentifierAsync(identifier: Identifier): Future[Ref.Identifier] = {
    IdentifierValidator
      .validateIdentifier(identifier, packageResolver)
      .transform {
        case Success(r) => Success(r)
        case Failure(NonFatal(t)) =>
          Failure(generalGrpcError(identifier))
        case fatal => fatal
      }(DirectExecutionContext)
  }
}

object IdentifierResolver {
  def apply(packageResolver: Ref.PackageId => Future[Option[Package]]): IdentifierResolver =
    new IdentifierResolver(packageResolver)
}
