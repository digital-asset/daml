// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver.services.admin

import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.localstore.api.IdentityProviderConfigStore
import com.daml.platform.localstore.api.IdentityProviderConfigStore.Result

import scala.concurrent.{ExecutionContext, Future}

trait IdentityProviderConfigValidation {

  def identityProviderConfigExists(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Unit]

}

class IdentityProviderConfigValidationImpl(
    identityProviderConfigStore: IdentityProviderConfigStore
)(implicit executionContext: ExecutionContext, loggingContext: LoggingContext)
    extends IdentityProviderConfigValidation {

  private implicit val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private implicit val contextualizedErrorLogger: DamlContextualizedErrorLogger =
    new DamlContextualizedErrorLogger(logger, loggingContext, None)

  def handleResult(operation: String, id: IdentityProviderId)(
      result: Result[IdentityProviderConfig]
  ): Future[Unit] = result match {
    case Right(_) => Future.successful(())
    case Left(_) =>
      Future.failed(
        LedgerApiErrors.Admin.IdentityProviderConfig.IdentityProviderConfigNotFound
          .Reject(operation, identityProviderId = id.toRequestString)
          .asGrpcError
      )
  }

  override def identityProviderConfigExists(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = id match {
    case IdentityProviderId.Default => Future.successful(())
    case dbId: IdentityProviderId.Id =>
      identityProviderConfigStore.getIdentityProviderConfig(dbId).flatMap(handleResult("", id))
  }
}
