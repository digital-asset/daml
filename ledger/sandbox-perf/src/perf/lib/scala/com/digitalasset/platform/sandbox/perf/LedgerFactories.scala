// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File
import java.util.concurrent.Executors
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.sandbox.SandboxServer
import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType.Static
import com.daml.ports.Port
import com.daml.testing.postgresql.PostgresResource

import java.time.Duration
import scala.concurrent.ExecutionContext

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader.assertReadFile(file).all.head.pkgId

  private def sandboxConfig(jdbcUrl: Option[String], darFiles: List[File]) =
    SandboxConfig.defaultConfig.copy(
      port = Port.Dynamic,
      damlPackages = darFiles,
      ledgerIdMode =
        LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString("ledger-server"))),
      jdbcUrl = jdbcUrl,
      timeProviderType = Some(Static),
      delayBeforeSubmittingLedgerConfiguration = Duration.ZERO,
    )

  val mem = "InMemory"
  val sql = "Postgres"

  def createSandboxResource(
      store: String,
      darFiles: List[File],
  )(implicit resourceContext: ResourceContext): Resource[LedgerContext] = new OwnedResource(
    for {
      executor <- ResourceOwner.forExecutorService(() => Executors.newSingleThreadExecutor())
      jdbcUrl <- store match {
        case `mem` =>
          ResourceOwner.successful(None)
        case `sql` =>
          PostgresResource.owner[ResourceContext]().map(database => Some(database.url))
      }
      port <- SandboxServer.owner(sandboxConfig(jdbcUrl, darFiles))
      channel <- GrpcClientResource.owner(port)
    } yield new LedgerContext(channel, darFiles.map(getPackageIdOrThrow))(
      ExecutionContext.fromExecutorService(executor)
    )
  )
}
