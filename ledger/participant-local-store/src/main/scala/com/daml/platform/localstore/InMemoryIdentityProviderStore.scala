// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore

import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.IdentityProviderConfig
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.IdentityProviderId
import com.daml.logging.LoggingContext
import com.daml.platform.localstore.api.IdentityProviderStore
import com.daml.platform.localstore.api.IdentityProviderStore.{
  IdentityProviderConfigNotFound,
  Result,
}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

class InMemoryIdentityProviderStore extends IdentityProviderStore {
  private val state: TrieMap[Ref.IdentityProviderId, IdentityProviderConfig] =
    TrieMap[Ref.IdentityProviderId, IdentityProviderConfig]()

  override def createIdentityProviderConfig(identityProviderConfig: domain.IdentityProviderConfig)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    state.put(identityProviderConfig.identityProviderId, identityProviderConfig)
    Future.successful(Right(identityProviderConfig))
  }

  override def getIdentityProviderConfig(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.IdentityProviderConfig]] = {
    Future.successful(state.get(id).toRight(IdentityProviderConfigNotFound(id)))
  }

  override def deleteIdentityProviderConfig(id: IdentityProviderId)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Unit]] = {
    state.remove(id)
    Future.successful(Right(()))
  }

  override def listIdentityProviderConfigs()(implicit
      loggingContext: LoggingContext
  ): Future[Result[Seq[domain.IdentityProviderConfig]]] = {
    Future.successful(Right(state.values.toSeq))
  }
}
