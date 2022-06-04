// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config
import com.daml.ledger.sandbox.SandboxOnXForTest._
import com.daml.ledger.sandbox.{
  BridgeConfig,
  BridgeConfigAdaptor,
  SandboxOnXForTest,
  SandboxOnXRunner,
}
import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.services.time.TimeProviderType
import com.daml.testing.postgresql.PostgresResource

import java.io.File
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader.assertReadFile(file).all.head.pkgId

  def bridgeConfig: BridgeConfig = BridgeConfig()

  protected def sandboxConfig(
      jdbcUrl: Option[String]
  ): Config = Default.copy(
    ledgerId = "ledger-server",
    participants = singleParticipant(
      ApiServerConfig.copy(
        seeding = Seeding.Weak,
        timeProviderType = TimeProviderType.Static,
      )
    ),
    dataSource = dataSource(jdbcUrl.getOrElse(SandboxOnXForTest.defaultH2SandboxJdbcUrl())),
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
      configAdaptor: BridgeConfigAdaptor = new ConfigAdaptor(
        None
      )

      port <- SandboxOnXRunner.owner(
        configAdaptor,
        sandboxConfig(jdbcUrl),
        bridgeConfig,
      )
      channel <- GrpcClientResource.owner(port)
    } yield new LedgerContext(channel, darFiles.map(getPackageIdOrThrow))(
      ExecutionContext.fromExecutorService(executor)
    )
  )
}
