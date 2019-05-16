// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.identity

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundEach
}
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.client.services.identity.LedgerIdentityClient
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

import scala.concurrent.Future
import scala.concurrent.duration._

trait LedgerIdentityServiceITBase
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundEach
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = 5.seconds

  protected lazy val givenId =
    Ref.LedgerString.assertFromString(s"ledger-${UUID.randomUUID().toString}")

  protected def getLedgerId(ledgerIdentityService: LedgerIdentityService): Future[String] = {
    val client = new LedgerIdentityClient(ledgerIdentityService)
    client.getLedgerId()
  }

}
