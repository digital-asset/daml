// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.time.Instant
import java.util.zip.ZipFile

import akka.stream.ActorMaterializer
import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.api.util.{TimeProvider, ToleranceWindow}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.sandbox.config.DamlPackageContainer
import com.digitalasset.platform.sandbox.damle.SandboxPackageStore
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.ApiSubmissionService
import com.digitalasset.platform.sandbox.stores.{
  ActiveContractsInMemory,
  SandboxIndexAndWriteService
}
import com.digitalasset.platform.sandbox.stores.ledger.CommandExecutorImpl
import com.digitalasset.platform.server.api.validation.IdentifierResolver
import com.digitalasset.platform.services.time.TimeModel

import scala.concurrent.{ExecutionContext, Future}

object TestDar {
  val darFile: File = new File("ledger/sandbox/Test.dar")
  lazy val parsedPackageId = DarReader().readArchive(new ZipFile(darFile)).get.main._1
}

trait TestHelpers {
  protected val packageStore = {
    val packageStore = SandboxPackageStore()
    packageStore.putDarFile(Instant.EPOCH, "", TestDar.darFile) match {
      case Right(details @ _) => ()
      case Left(err) => sys.error(s"Could not load package ${TestDar.darFile}: $err")
    }
    packageStore
  }

  // TODO: change damle flag to LF once finished implementation
  protected def submissionService(timeProvider: TimeProvider, toleranceWindow: ToleranceWindow)(
      implicit ec: ExecutionContext,
      mat: ActorMaterializer) = {

    implicit val mm: MetricsManager = MetricsManager()

    val ledgerId = LedgerId("sandbox-ledger")

    val indexAndWriteService = SandboxIndexAndWriteService
      .inMemory(
        ledgerId,
        TimeModel.reasonableDefault,
        TimeProvider.Constant(Instant.EPOCH),
        ActiveContractsInMemory.empty,
        ImmArray.empty,
        packageStore
      )

    ApiSubmissionService.create(
      ledgerId,
      IdentifierResolver(packageStore.getLfPackage),
      indexAndWriteService.indexService,
      indexAndWriteService.writeService,
      TimeModel.reasonableDefault,
      timeProvider,
      new CommandExecutorImpl(Engine(), packageStore.getLfPackage)
    )(ec, mat)
  }

}
