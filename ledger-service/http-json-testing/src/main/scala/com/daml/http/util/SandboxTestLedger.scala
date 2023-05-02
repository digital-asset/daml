// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
// import com.daml.platform.sandbox.SandboxRequiringAuthorizationFuns
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import org.scalatest.Suite

import scala.concurrent.{ExecutionContext, Future}

import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.lf.integrationtest.CantonFixture
import scala.annotation.nowarn

trait SandboxTestLedger extends CantonFixture {
  self: Suite =>

  protected def testId: String
  protected def packageFiles: List[java.io.File] = List()

  def useTls: UseTls

  override protected def authSecret: Option[String] = None
  override protected def darFiles = packageFiles.map(_.toPath)
  override protected def devMode = false
  override protected def nParticipants = 1
  override protected def timeProviderType = TimeProviderType.WallClock
  override protected def tlsEnable = useTls
  override protected def applicationId: ApplicationId = ApplicationId("sandbox-test-ledger")
  override protected def enableDisclosedContracts: Boolean = true

  def usingLedger[A](@nowarn testName: String, token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = {
    val client = defaultLedgerClientWithoutId(token)
    testFn(suiteResource.value.head, client, LedgerId("participant0"))
  }
}
