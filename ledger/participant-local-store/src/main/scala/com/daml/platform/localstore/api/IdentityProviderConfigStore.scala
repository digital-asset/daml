// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result

import scala.concurrent.Future

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
}
