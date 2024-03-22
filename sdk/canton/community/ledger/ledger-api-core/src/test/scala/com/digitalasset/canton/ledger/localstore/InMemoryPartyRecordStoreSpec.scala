// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.localstore.InMemoryPartyRecordStore
import com.digitalasset.canton.ledger.localstore.api.PartyRecordStore
import org.scalatest.freespec.AsyncFreeSpec

import scala.concurrent.Future

class InMemoryPartyRecordStoreSpec extends AsyncFreeSpec with PartyRecordStoreTests with BaseTest {

  override def newStore(): PartyRecordStore = new InMemoryPartyRecordStore(
    executionContext = executionContext,
    loggerFactory = loggerFactory,
  )

  override def createIdentityProviderConfig(
      identityProviderConfig: domain.IdentityProviderConfig
  ): Future[Unit] = Future.unit
}
