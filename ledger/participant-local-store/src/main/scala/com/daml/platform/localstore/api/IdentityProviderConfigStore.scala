// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result

import scala.concurrent.{ExecutionContext, Future}

trait IdentityProviderConfigStore {

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]]

  def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[IdentityProviderConfig]]]

  def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean]

  final def getActiveIdentityProviderByIssuer(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[IdentityProviderConfig] =
    getIdentityProviderConfig(issuer)
      .flatMap {
        case Right(value) if !value.isDeactivated => Future.successful(value)
        case Right(value) =>
          Future.failed(
            new Exception(s"Identity Provider ${value.identityProviderId.value} is deactivated.")
          )
        case Left(error) =>
          Future.failed(new Exception(error.toString))
      }(ExecutionContext.parasitic)

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
