// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import akka.actor.ActorSystem
import akka.stream._
import ch.qos.logback.classic.Level
import com.daml.auth.TokenHolder
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.archive.{Dar, DarDecoder}
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast._
import com.daml.lf.language.PackageInterface
import com.daml.scalautil.Statement.discard

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object RunnerMain {

  private def listTriggers(
      darPath: File,
      dar: Dar[(PackageId, Package)],
      verbose: Boolean,
  ): Unit = {
    println(s"Listing triggers in $darPath:")
    val pkgInterface = PackageInterface(dar.all.toMap)
    val (mainPkgId, mainPkg) = dar.main
    for {
      mod <- mainPkg.modules.values
      defName <- mod.definitions.keys
      qualifiedName = QualifiedName(mod.name, defName)
      triggerId = Identifier(mainPkgId, qualifiedName)
    } {
      Trigger.detectTriggerDefinition(pkgInterface, triggerId).foreach {
        case TriggerDefinition(_, ty, version, level, _) =>
          if (verbose)
            println(
              s"  $qualifiedName\t(type = ${ty.pretty}, level = $level, version = $version)"
            )
          else
            println(s"  $qualifiedName")
      }
    }
  }

  def main(args: Array[String]): Unit = {

    RunnerConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => {
        config.rootLoggingLevel.foreach(setLoggingLevel)
        config.logEncoder match {
          case LogEncoder.Plain =>
          case LogEncoder.Json =>
            discard(System.setProperty("LOG_FORMAT_JSON", "true"))
        }

        val dar: Dar[(PackageId, Package)] =
          DarDecoder.assertReadArchiveFromFile(config.darPath.toFile)

        config.listTriggers.foreach { verbose =>
          listTriggers(config.darPath.toFile, dar, verbose)
          sys.exit(0)
        }

        val triggerId: Identifier =
          Identifier(dar.main._1, QualifiedName.assertFromString(config.triggerIdentifier))

        val system: ActorSystem = ActorSystem("TriggerRunner")
        implicit val materializer: Materializer = Materializer(system)
        val sequencer = new AkkaExecutionSequencerPool("TriggerRunnerPool")(system)
        implicit val ec: ExecutionContext = system.dispatcher

        val tokenHolder = config.accessTokenFile.map(new TokenHolder(_))
        // We probably want to refresh the token at some point but given that triggers
        // are expected to be written such that they can be killed and restarted at
        // any time it would in principle also be fine to just have the auth failure due
        // to an expired token tear the trigger down and have some external monitoring process (e.g. systemd)
        // restart it.
        val clientConfig = LedgerClientConfiguration(
          applicationId = ApplicationId.unwrap(config.applicationId),
          ledgerIdRequirement = LedgerIdRequirement.none,
          commandClient =
            CommandClientConfiguration.default.copy(defaultDeduplicationTime = config.commandTtl),
          token = tokenHolder.flatMap(_.token),
        )

        val channelConfig = LedgerClientChannelConfiguration(
          sslContext = config.tlsConfig.client(),
          maxInboundMessageSize = config.maxInboundMessageSize,
        )

        val flow: Future[Unit] = for {
          client <- LedgerClient.singleHost(
            config.ledgerHost,
            config.ledgerPort,
            clientConfig,
            channelConfig,
          )(ec, sequencer)

          parties <- config.ledgerClaims.resolveClaims(client)

          _ <- Runner.run(
            dar,
            triggerId,
            client,
            config.timeProviderType.getOrElse(RunnerConfig.DefaultTimeProviderType),
            config.applicationId,
            parties,
            config.compilerConfig,
            config.triggerConfig,
          )
        } yield ()

        flow.onComplete(_ => system.terminate())

        Await.result(flow, Duration.Inf)
      }
    }
  }

  private def setLoggingLevel(level: Level): Unit = {
    discard(System.setProperty("LOG_LEVEL_ROOT", level.levelStr))
  }
}
