// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.localstore.api

import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLogging}
import com.digitalasset.canton.platform.localstore.api.IdentityProviderConfigStore.Result

import scala.concurrent.{ExecutionContext, Future}

trait IdentityProviderConfigStore { self: NamedLogging =>

  protected val directEc: ExecutionContext = DirectExecutionContext(noTracingLogger)

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]]

  def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]]

  def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Unit]]

  def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Seq[IdentityProviderConfig]]]

  def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]]

  def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[IdentityProviderConfig]]

  def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Boolean]

  final def getActiveIdentityProviderByIssuer(issuer: String)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[IdentityProviderConfig] =
    getIdentityProviderConfig(issuer)
      .flatMap {
        case Right(value) if !value.isDeactivated => Future.successful(value)
        case Right(value) =>
          // We do not throw here an error code, as this code path is
          // handled by IdentityProviderAwareAuthService by transforming the
          // exception into a warning in the logs.
          Future.failed(
            new Exception(s"Identity Provider ${value.identityProviderId.value} is deactivated.")
          )
        case Left(error) =>
          Future.failed(new Exception(error.toString))
      }(directEc)
}

object IdentityProviderConfigStore {

  type Result[T] = Either[Error, T]

  sealed trait Error
  final case class IdentityProviderConfigNotFound(identityProviderId: IdentityProviderId.Id)
      extends Error
  final case class IdentityProviderConfigExists(identityProviderId: IdentityProviderId.Id)
      extends Error
  final case class IdentityProviderConfigWithIssuerExists(issuer: String) extends Error
  final case class TooManyIdentityProviderConfigs() extends Error
  final case class IdentityProviderConfigByIssuerNotFound(issuer: String) extends Error
}
