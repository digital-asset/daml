// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.perf

import java.io.File

import akka.stream.Materializer
import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services.{SandboxClientResource, SandboxServerResource}
import com.digitalasset.testing.postgresql.{PostgresFixture, PostgresResource}

object LedgerFactories {

  private def getPackageIdOrThrow(file: File): Ref.PackageId =
    UniversalArchiveReader().readFile(file).map(_.all.head._1).get

  private def sandboxConfig(jdbcUrl: Option[String], darFiles: List[File]) =
    SandboxConfig.default.copy(
      port = 0,
      damlPackages = darFiles,
      ledgerIdMode =
        LedgerIdMode.Static(LedgerId(Ref.LedgerString.assertFromString("ledger-server"))),
      jdbcUrl = jdbcUrl,
    )

  val mem = "InMemory"
  val sql = "Postgres"

  def createSandboxResource(store: String, darFiles: List[File])(
      implicit esf: ExecutionSequencerFactory,
      mat: Materializer): Resource[LedgerContext] = {

    def createClientResource(port: Int): Resource[LedgerContext] =
      new SandboxClientResource(port).map(new LedgerContext(_, darFiles.map(getPackageIdOrThrow)))

    store match {
      case `mem` =>
        new Resource[LedgerContext] {
          @volatile private var server: Resource[SandboxServer] = _
          @volatile private var client: Resource[LedgerContext] = _

          override def value: LedgerContext = client.value

          override def setup(): Unit = {
            server = SandboxServerResource(sandboxConfig(None, darFiles))
            server.setup()
            client = createClientResource(server.value.port)
            client.setup()
          }

          override def close(): Unit = {
            client.close()
            client = null
            server.close()
            server = null
          }
        }
      case `sql` =>
        new Resource[LedgerContext] {
          @volatile private var postgres: Resource[PostgresFixture] = _
          @volatile private var server: Resource[SandboxServer] = _
          @volatile private var client: Resource[LedgerContext] = _

          override def value: LedgerContext = client.value

          override def setup(): Unit = {
            postgres = PostgresResource()
            postgres.setup()
            server = SandboxServerResource(sandboxConfig(Some(postgres.value.jdbcUrl), darFiles))
            server.setup()
            client = createClientResource(server.value.port)
            client.setup()
          }

          override def close(): Unit = {
            client.close()
            client = null
            server.close()
            server = null
            postgres.close()
            postgres = null
          }
        }
    }

  }
}
