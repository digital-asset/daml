// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.time.Instant

import akka.stream.ActorMaterializer
import com.digitalasset.api.util.{TimeProvider, ToleranceWindow}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.services.SandboxSubmissionService
import com.digitalasset.platform.sandbox.stores.ActiveContractsInMemory
import com.digitalasset.platform.sandbox.stores.ledger.{
  CommandExecutorImpl,
  Ledger,
  SandboxLedgerBackend
}
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.services.time.TimeModel

import scala.concurrent.{ExecutionContext, Future}

object TestDar {
  val dalfFile: File = new File("ledger/sandbox/Test.dar")
  // DamlLf1 test package
  lazy val parsedPackage = DamlPackageContainer(List(dalfFile))
  lazy val parsedArchive = parsedPackage.archives.head
  lazy val parsedPackageId: String = parsedArchive.getHash

}

trait TestHelpers {
  protected val damlPackageContainer = TestDar.parsedPackage

  // TODO: change damle flag to LF once finished implementation
  protected def submissionService(timeProvider: TimeProvider, toleranceWindow: ToleranceWindow)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer) = {
    val ledger = Ledger.inMemory(
      "sandbox ledger",
      TimeProvider.Constant(Instant.EPOCH),
      ActiveContractsInMemory.empty,
      Nil)
    SandboxSubmissionService.createApiService(
      damlPackageContainer,
      IdentifierResolver(pkgId => Future.successful(damlPackageContainer.getPackage(pkgId))),
      new SandboxLedgerBackend(ledger),
      TimeModel.reasonableDefault,
      timeProvider,
      new CommandExecutorImpl(Engine(), damlPackageContainer)
    )(ec, mat)
  }

}
