// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{IdentityProviderConfig, IdentityProviderId}
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderConfigStore._
import com.daml.platform.localstore.api.{IdentityProviderConfigStore, IdentityProviderConfigUpdate}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryIdentityProviderConfigStore(maxIdentityProviderConfigs: Int = 10)
    extends IdentityProviderConfigStore {
  private val state: TrieMap[IdentityProviderId.Id, IdentityProviderConfig] =
    TrieMap[IdentityProviderId.Id, IdentityProviderConfig]()

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = withState {
    for {
      _ <- checkIssuerDoNotExists(
        identityProviderConfig.issuer,
        identityProviderConfig.identityProviderId,
      )
      _ <- checkIdDoNotExists(identityProviderConfig.identityProviderId)
      _ <- tooManyIdentityProviderConfigs()
      _ = state.put(identityProviderConfig.identityProviderId, identityProviderConfig)
    } yield identityProviderConfig
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
      _ <- update.issuerUpdate
        .map(checkIssuerDoNotExists(_, update.identityProviderId))
        .getOrElse(Right(()))
    } yield {
      val updatedValue = currentState
        .copy(isDeactivated = update.isDeactivatedUpdate.getOrElse(currentState.isDeactivated))
        .copy(issuer = update.issuerUpdate.getOrElse(currentState.issuer))
        .copy(jwksUrl = update.jwksUrlUpdate.getOrElse(currentState.jwksUrl))
      state.put(update.identityProviderId, updatedValue)
      updatedValue
    }
  }

  override def getIdentityProviderConfig(issuer: String)(implicit
      loggingContext: LoggingContext
  ): Future[Result[IdentityProviderConfig]] = withState {
    state
      .collectFirst { case (_, config) if config.issuer == issuer => Right(config) }
      .getOrElse(Left(IdentityProviderConfigByIssuerNotFound(issuer)))
  }

  override def identityProviderConfigExists(id: IdentityProviderId.Id)(implicit
      loggingContext: LoggingContext
  ): Future[Boolean] = withState {
    state.isDefinedAt(id)
  }

  private def checkIssuerDoNotExists(
      issuer: String,
      idToIgnore: IdentityProviderId.Id,
  ): Result[Unit] =
    Either.cond(
      !state.values.exists(cfg => cfg.issuer == issuer && cfg.identityProviderId != idToIgnore),
      (),
      IdentityProviderConfigWithIssuerExists(issuer),
    )

  private def checkIdDoNotExists(id: IdentityProviderId.Id): Result[Unit] =
    Either.cond(
      !state.isDefinedAt(id),
      (),
      IdentityProviderConfigExists(id),
    )

  private def tooManyIdentityProviderConfigs(): Result[Unit] = {
    Either.cond(
      state.size + 1 <= maxIdentityProviderConfigs,
      (),
      TooManyIdentityProviderConfigs(),
    )
  }

  private def checkIdExists(id: IdentityProviderId.Id): Result[IdentityProviderConfig] =
    state.get(id).toRight(IdentityProviderConfigNotFound(id))

  private def withState[T](t: => T): Future[T] =
    state.synchronized(
      Future.successful(t)
    )

}
