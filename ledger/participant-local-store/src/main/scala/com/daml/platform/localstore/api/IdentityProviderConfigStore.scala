// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result

import scala.concurrent.Future

trait IdentityProviderConfigStore {

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def getIdentityProviderConfig(id: Ref.IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]]

  def deleteIdentityProviderConfig(id: Ref.IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]]

  def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[IdentityProviderConfig]]]

}
object IdentityProviderConfigStore {
  type Result[T] = Either[Error, T]

  sealed trait Error
  final case class IdentityProviderConfigNotFound(identityProviderId: Ref.IdentityProviderId.Id)
      extends Error
  final case class IdentityProviderConfigExists(identityProviderId: Ref.IdentityProviderId.Id)
      extends Error
}
