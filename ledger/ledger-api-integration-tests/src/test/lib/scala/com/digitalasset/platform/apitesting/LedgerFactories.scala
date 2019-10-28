// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.nio.file.Path

import com.digitalasset.daml.lf.archive.UniversalArchiveReader
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.LedgerFactories.SandboxStore.InMemory
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.persistence.{PostgresFixture, PostgresResource}

object LedgerFactories {

  private def getPackageIdOrThrow(path: Path): Ref.PackageId =
    UniversalArchiveReader().readFile(path.toFile).map(_.all.head._1).get

  sealed abstract class SandboxStore extends Product with Serializable

  object SandboxStore {

    case object InMemory extends SandboxStore

    case object Postgres extends SandboxStore

    // meant to be used purely for JMH benchmarks
    def apply(s: String): SandboxStore = s match {
      case "InMemory" => SandboxStore.InMemory
      case "Postgres" => SandboxStore.Postgres
    }

  }

  def createRemoteApiProxyResource(config: PlatformApplications.Config)(
      implicit esf: ExecutionSequencerFactory): Resource[LedgerContext.SingleChannelContext] = {
    require(config.remoteApiEndpoint.isDefined, "config.remoteApiEndpoint has to be set")
    val endpoint = config.remoteApiEndpoint.get
    val packageIds = config.darFiles.map(getPackageIdOrThrow)

    RemoteServerResource(endpoint.host, endpoint.port, endpoint.tlsConfig)
      .map {
        case PlatformChannels(channel) =>
          LedgerContext.SingleChannelContext(channel, None, config.ledgerId, packageIds)
      }
  }

  def createSandboxResource(config: PlatformApplications.Config, store: SandboxStore = InMemory)(
      implicit esf: ExecutionSequencerFactory): Resource[LedgerContext.SingleChannelContext] = {
    val packageIds = config.darFiles.map(getPackageIdOrThrow)

    def createResource(sandboxConfig: SandboxConfig) =
      SandboxServerResource(sandboxConfig).map {
        case PlatformChannels(channel) =>
          LedgerContext.SingleChannelContext(channel, None, config.ledgerId, packageIds)
      }

    store match {
      case SandboxStore.InMemory =>
        createResource(PlatformApplications.sandboxConfig(config, None))
      case SandboxStore.Postgres =>
        new Resource[LedgerContext.SingleChannelContext] {
          @volatile
          private var postgres: Resource[PostgresFixture] = null

          @volatile
          private var sandbox: Resource[LedgerContext.SingleChannelContext] = null

          override def value(): LedgerContext.SingleChannelContext = sandbox.value

          override def setup(): Unit = {
            postgres = PostgresResource()
            postgres.setup()
            sandbox = createResource(
              PlatformApplications.sandboxConfig(config, Some(postgres.value.jdbcUrl)))
            sandbox.setup()
          }

          override def close(): Unit = {
            sandbox.close()
            postgres.close()

            sandbox = null
            postgres = null
          }
        }
    }

  }
}
