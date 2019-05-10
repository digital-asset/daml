// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.file.Path

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.Resource
import com.digitalasset.platform.PlatformApplications
import com.digitalasset.platform.apitesting.LedgerFactories.SandboxStore.InMemory
import com.digitalasset.platform.damllf.PackageParser
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.persistence.{PostgresFixture, PostgresResource}

import scala.util.control.NonFatal

object LedgerFactories {

  private def packageIdFromString(str: String): Either[Throwable, Ref.PackageId] =
    Ref.PackageId.fromString(str) match {
      case Left(e) => Left(new IllegalStateException(e))
      case Right(x) => Right(x)
    }

  private def getPackageId(path: Path): Either[Throwable, Ref.PackageId] = {
    val inputStream: InputStream = new BufferedInputStream(new FileInputStream(path.toFile))
    try {
      if (path.toFile.getName.endsWith(".dalf")) {
        PackageParser.getPackageIdFromDalf(inputStream)
      } else
        PackageParser.getPackageIdFromDar(inputStream).flatMap(packageIdFromString)
    } catch {
      case NonFatal(t) => throw new RuntimeException(s"Couldn't parse ${path}", t)
    }
  }

  private def getPackageIdOrThrow(path: Path): Ref.PackageId =
    getPackageId(path).fold(t => throw t, identity)

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

  def createSandboxResource(config: PlatformApplications.Config, store: SandboxStore = InMemory)(
      implicit esf: ExecutionSequencerFactory): Resource[LedgerContext.SingleChannelContext] = {
    val packageIds = config.darFiles.map(getPackageIdOrThrow)

    def createResource(sandboxConfig: SandboxConfig) =
      SandboxServerResource(sandboxConfig).map {
        case PlatformChannels(channel) =>
          LedgerContext.SingleChannelContext(channel, config.ledgerId, packageIds)
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
