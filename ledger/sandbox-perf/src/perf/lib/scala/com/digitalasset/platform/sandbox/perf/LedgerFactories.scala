// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File
import java.util.concurrent.Executors

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.services.time.TimeProviderType.Static
import com.daml.ports.Port
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.ExecutionContext

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader.assertReadFile(file).all.head.pkgId

  private def sandboxConfig(jdbcUrl: Option[String], darFiles: List[File]) =
    sandbox.DefaultConfig.copy(
      port = Port.Dynamic,
      damlPackages = darFiles,
      ledgerIdMode =
        LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString("ledger-server"))),
      jdbcUrl = jdbcUrl,
      timeProviderType = Some(Static),
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
      server <- SandboxServer.owner(sandboxConfig(jdbcUrl, darFiles))
      channel <- GrpcClientResource.owner(server.port)
    } yield new LedgerContext(channel, darFiles.map(getPackageIdOrThrow))(
      ExecutionContext.fromExecutorService(executor)
    )
  )
}
