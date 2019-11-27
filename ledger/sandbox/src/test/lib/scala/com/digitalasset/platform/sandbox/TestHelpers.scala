// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox

import java.io.File
import java.time.Instant

import akka.stream.ActorMaterializer
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{ParticipantId, TimeModel}
import com.digitalasset.api.util.{TimeProvider, ToleranceWindow}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.sandbox.services.ApiSubmissionService
import com.digitalasset.platform.sandbox.stores.ledger.CommandExecutorImpl
import com.digitalasset.platform.sandbox.stores.{
  InMemoryActiveLedgerState,
  InMemoryPackageStore,
  SandboxIndexAndWriteService
}

import scala.concurrent.ExecutionContext

object TestDar {
  val darFile: File = new File("ledger/test-common/Test-stable.dar")
  lazy val parsedPackageId = DarReader()
    .readArchiveFromFile(darFile)
    .get
    .main
    ._1
}

trait TestHelpers {
  protected val packageStore = {
    val packageStore = InMemoryPackageStore.empty
    packageStore.withDarFile(Instant.EPOCH, None, TestDar.darFile) match {
      case Right(details @ _) => ()
      case Left(err) => sys.error(s"Could not load package ${TestDar.darFile}: $err")
    }
    packageStore
  }

  // TODO: change damle flag to LF once finished implementation
  protected def submissionService(
      timeProvider: TimeProvider,
      toleranceWindow: ToleranceWindow,
      authService: AuthService)(implicit ec: ExecutionContext, mat: ActorMaterializer) = {

    val ledgerId = LedgerId("sandbox-ledger")
    val participantId: ParticipantId = Ref.LedgerString.assertFromString("sandbox-participant")
    val metrics = new MetricRegistry

    val indexAndWriteService = SandboxIndexAndWriteService
      .inMemory(
        ledgerId,
        participantId,
        TimeModel.reasonableDefault,
        TimeProvider.Constant(Instant.EPOCH),
        InMemoryActiveLedgerState.empty,
        ImmArray.empty,
        packageStore,
        metrics
      )

    ApiSubmissionService.create(
      ledgerId,
      indexAndWriteService.indexService,
      indexAndWriteService.writeService,
      TimeModel.reasonableDefault,
      timeProvider,
      new CommandExecutorImpl(Engine(), packageStore.getLfPackage),
      NamedLoggerFactory.forParticipant(participantId),
      metrics
    )(ec, mat)
  }

}
