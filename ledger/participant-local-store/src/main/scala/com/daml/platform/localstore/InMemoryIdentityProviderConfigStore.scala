// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore.{
  IdentityProviderConfigExists,
  IdentityProviderConfigNotFound,
  IdentityProviderConfigWithIssuerExists,
  Result,
}
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryIdentityProviderConfigStore extends IdentityProviderConfigStore {
  private val state: TrieMap[IdentityProviderId.Id, IdentityProviderConfig] =
    TrieMap[IdentityProviderId.Id, IdentityProviderConfig]()

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = withState {
    for {
      _ <- checkIssuerDoNotExists(identityProviderConfig.issuer)
      _ <- checkIdDoNotExists(identityProviderConfig.identityProviderId)
    } yield {
      state.put(identityProviderConfig.identityProviderId, identityProviderConfig)
      identityProviderConfig
    }
  }

  override def getIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = withState {
    state.get(id).toRight(IdentityProviderConfigNotFound(id))
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] = withState {
    for {
      _ <- checkIdExists(id)
    } yield {
      state.remove(id)
      ()
    }
  }

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[domain.IdentityProviderConfig]]] = withState {
    Right(state.values.toSeq)
  }

  override def updateIdentityProviderConfig(update: IdentityProviderConfigUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = withState {
    val id = update.identityProviderId
    for {
      currentState <- checkIdExists(id)
      _ <- update.issuerUpdate.map(checkIssuerDoNotExists).getOrElse(Right(()))
    } yield {
      val updatedValue = currentState
        .copy(isDeactivated = update.isDeactivatedUpdate.getOrElse(currentState.isDeactivated))
        .copy(issuer = update.issuerUpdate.getOrElse(currentState.issuer))
        .copy(jwksUrl = update.jwksUrlUpdate.getOrElse(currentState.jwksUrl))
      state.put(update.identityProviderId, updatedValue)
      updatedValue
    }
  }

  def checkIssuerDoNotExists(issuer: String): Result[Unit] =
    Either.cond(
      !state.values.exists(_.issuer == issuer),
      (),
      IdentityProviderConfigWithIssuerExists(issuer),
    )

  def checkIdDoNotExists(id: IdentityProviderId.Id): Result[Unit] =
    Either.cond(
      !state.isDefinedAt(id),
      (),
      IdentityProviderConfigExists(id),
    )

  def checkIdExists(id: IdentityProviderId.Id): Result[IdentityProviderConfig] =
    state.get(id).toRight(IdentityProviderConfigNotFound(id))

  private def withState[T](t: => T): Future[T] =
    state.synchronized(
      Future.successful(t)
    )

}
