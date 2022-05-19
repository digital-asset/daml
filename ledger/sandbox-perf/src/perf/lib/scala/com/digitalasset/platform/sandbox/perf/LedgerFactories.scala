// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.perf

import java.io.File
import java.util.concurrent.Executors
import com.daml.ledger.api.testing.utils.{OwnedResource, Resource}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.runner.common.Config.{SandboxParticipantConfig, SandboxParticipantId}
import com.daml.ledger.sandbox.{BridgeConfig, ConfigConverter, NewSandboxServer}
import com.daml.lf.archive.UniversalArchiveReader
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.services.GrpcClientResource
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.testing.postgresql.PostgresResource

import scala.concurrent.ExecutionContext

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader.assertReadFile(file).all.head.pkgId

  protected def sandboxConfig(
      jdbcUrl: Option[String],
      darFiles: List[File],
  ): NewSandboxServer.CustomConfig = NewSandboxServer.CustomConfig(
    genericConfig = com.daml.ledger.runner.common.Config.SandboxDefault.copy(
      ledgerId = "ledger-server",
      participants = Map(
        SandboxParticipantId -> SandboxParticipantConfig.copy(apiServer =
          SandboxParticipantConfig.apiServer.copy(
            seeding = Seeding.Weak,
            timeProviderType = TimeProviderType.Static,
          )
        )
      ),
      dataSource = Map(
        SandboxParticipantId -> ParticipantDataSourceConfig(
          jdbcUrl.getOrElse(ConfigConverter.defaultH2SandboxJdbcUrl())
        )
      ),
    ),
    bridgeConfig = BridgeConfig(),
    damlPackages = darFiles,
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
      port <- NewSandboxServer.owner(sandboxConfig(jdbcUrl, darFiles))
      channel <- GrpcClientResource.owner(port)
    } yield new LedgerContext(channel, darFiles.map(getPackageIdOrThrow))(
      ExecutionContext.fromExecutorService(executor)
    )
  )
}
