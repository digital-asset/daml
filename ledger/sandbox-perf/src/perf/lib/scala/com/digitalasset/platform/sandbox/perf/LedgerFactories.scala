// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File

import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.GrpcClientResource
import com.daml.platform.services.time.TimeProviderType.Static
import com.daml.ports.Port
import com.daml.resources.ResourceOwner
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.ExecutionContext

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader().readFile(file).map(_.all.head._1).get

  private def sandboxConfig(jdbcUrl: Option[String], darFiles: List[File]) =
    SandboxConfig.default.copy(
      port = Port.Dynamic,
      damlPackages = darFiles,
      ledgerIdMode =
        LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString("ledger-server"))),
      jdbcUrl = jdbcUrl,
      timeProviderType = Some(Static),
    )

  val mem = "InMemory"
  val sql = "Postgres"

  def createSandboxResource(store: String, darFiles: List[File])(
      implicit executionContext: ExecutionContext
  ): Resource[LedgerContext] =
    new OwnedResource(
      for {
        jdbcUrl <- store match {
          case `mem` =>
            ResourceOwner.successful(None)
          case `sql` =>
            PostgresResource.owner().map(database => Some(database.url))
        }
        server <- SandboxServer.owner(sandboxConfig(jdbcUrl, darFiles))
        channel <- GrpcClientResource.owner(server.port)
      } yield new LedgerContext(channel, darFiles.map(getPackageIdOrThrow))
    )
}
