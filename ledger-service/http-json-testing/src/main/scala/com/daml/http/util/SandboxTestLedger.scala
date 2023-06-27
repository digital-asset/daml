// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.bazeltools.BazelRunfiles._
import com.daml.http.HttpServiceTestFixture.UseTls
import com.daml.integrationtest.{CantonFixture, CantonRunner}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.client.withoutledgerid.{LedgerClient => DamlLedgerClient}
import com.daml.ports.Port
import org.scalatest.Suite
import org.scalatest.OptionValues._
import java.nio.file.{Files, Paths}

import scala.concurrent.{ExecutionContext, Future}

trait SandboxTestLedger extends CantonFixture {
  self: Suite =>

  protected def testId: String
  protected def packageFiles: List[java.io.File] = List()

  def useTls: UseTls

  override lazy protected val darFiles = packageFiles.map(_.toPath)
  override lazy protected val tlsEnable = useTls
  override lazy protected val enableDisclosedContracts: Boolean = true
  override lazy protected val cantonJar = {
    val eePath = Paths.get(rlocation("canton/canton-ee_deploy.jar"))
    if (Files.exists(eePath)) eePath else CantonRunner.cantonPath
  }

  def usingLedger[A](token: Option[String] = None)(
      testFn: (Port, DamlLedgerClient, LedgerId) => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = {
    val client = defaultLedgerClientWithoutId(token)
    testFn(ports.head, client, LedgerId(config.ledgerIds.headOption.value))
  }
}
