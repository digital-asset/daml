// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package util

import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.integrationtest.{CantonFixture}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ports.Port
import org.scalatest.Suite
import org.scalatest.OptionValues._

import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends CantonFixture {
  self: Suite =>

  protected def testId: String
  protected def packageFiles: List[java.io.File] = List()

  def useTls: UseTls

  final override protected lazy val cantonJar = Edition.cantonJar
  override lazy protected val darFiles = packageFiles.map(_.toPath)
  override lazy protected val tlsEnable = useTls
  override lazy protected val bootstrapScript = Some(
    "local.service.set_reconciliation_interval(1.seconds)"
  )

  def usingLedger[A](token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = {
    val client = defaultLedgerClientWithoutId(token)
    testFn(ports.head, client, LedgerId(config.ledgerIds.headOption.value))
  }
}
