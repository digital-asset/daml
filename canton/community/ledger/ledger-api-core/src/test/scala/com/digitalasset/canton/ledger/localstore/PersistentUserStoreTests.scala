// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.ledger.api.domain.IdentityProviderConfig
import com.digitalasset.canton.ledger.api.util.TimeProvider
import com.digitalasset.canton.ledger.localstore.api.UserManagementStore
import com.digitalasset.canton.ledger.localstore.{
  PersistentIdentityProviderConfigStore,
  PersistentUserManagementStore,
}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.store.backend.StorageBackendProvider
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

trait PersistentUserStoreTests extends PersistentStoreSpecBase with UserStoreTests {
  self: AsyncFreeSpec with StorageBackendProvider =>

  override def newStore(): UserManagementStore =
    new PersistentUserManagementStore(
      dbSupport = dbSupport,
      metrics = Metrics.ForTesting,
      timeProvider = TimeProvider.UTC,
      maxRightsPerUser = 100,
      loggerFactory = loggerFactory,
    )

  def createIdentityProviderConfig(identityProviderConfig: IdentityProviderConfig): Future[Unit] = {
    new PersistentIdentityProviderConfigStore(dbSupport, Metrics.ForTesting, 10, loggerFactory)
      .createIdentityProviderConfig(identityProviderConfig)(loggingContext)
      .flatMap {
        case Left(error) => Future.failed(new Exception(error.toString))
        case Right(_) => Future.unit
      }
  }
}
