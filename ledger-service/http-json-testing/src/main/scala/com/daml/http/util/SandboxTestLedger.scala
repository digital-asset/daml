// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ports.Port
import org.scalatest.Suite
import org.scalatest.OptionValues._

import scala.concurrent.{ExecutionContext, Future}

import com.daml.lf.integrationtest.CantonFixture

trait SandboxTestLedger extends CantonFixture {
  self: Suite =>

  protected def testId: String
  protected def packageFiles: List[java.io.File] = List()

  def useTls: UseTls

  override lazy protected val darFiles = packageFiles.map(_.toPath)
  override lazy protected val tlsEnable = useTls
  override lazy protected val enableDisclosedContracts: Boolean = true

  def usingLedger[A](token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = {
    val client = defaultLedgerClientWithoutId(token)
    testFn(ports.head, client, LedgerId(config.ledgerIds.headOption.value))
  }
}
