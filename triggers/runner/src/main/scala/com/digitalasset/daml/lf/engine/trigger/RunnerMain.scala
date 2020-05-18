// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import akka.actor.ActorSystem
import akka.stream._
import java.io.File
import io.grpc.netty.NettyChannelBuilder
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scalaz.syntax.traverse._

import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref.{Identifier, PackageId, QualifiedName}
import com.daml.lf.language.Ast._
import com.daml.daml_lf_dev.DamlLf
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.auth.TokenHolder

object RunnerMain {

  def listTriggers(darPath: File, dar: Dar[(PackageId, Package)]) = {
    println(s"Listing triggers in $darPath:")
    for ((modName, mod) <- dar.main._2.modules) {
      for ((defName, defVal) <- mod.definitions) {
        defVal match {
          case DValue(TApp(TTyCon(tcon), _), _, _, _) => {
            val triggerIds = TriggerIds(tcon.packageId)
            if (tcon == triggerIds.damlTrigger("Trigger")
              || tcon == triggerIds.damlTriggerLowLevel("Trigger")) {
              println(s"  $modName:$defName")
            }
          }
          case _ => {}
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {

    RunnerConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => {
        val encodedDar: Dar[(PackageId, DamlLf.ArchivePayload)] =
          DarReader().readArchiveFromFile(config.darPath.toFile).get
        val dar: Dar[(PackageId, Package)] = encodedDar.map {
          case (pkgId, pkgArchive) => Decode.readArchivePayload(pkgId, pkgArchive)
        }

        if (config.listTriggers) {
          listTriggers(config.darPath.toFile, dar)
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
        val applicationId = ApplicationId("Trigger Runner")
        val clientConfig = LedgerClientConfiguration(
          applicationId = ApplicationId.unwrap(applicationId),
          ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
          commandClient =
            CommandClientConfiguration.default.copy(defaultDeduplicationTime = config.commandTtl),
          sslContext = config.tlsConfig.flatMap(_.client),
          token = tokenHolder.flatMap(_.token)
        )

        val flow: Future[Unit] = for {
          client <- LedgerClient
            .fromBuilder(
              NettyChannelBuilder
                .forAddress(config.ledgerHost, config.ledgerPort)
                .maxInboundMessageSize(config.maxInboundMessageSize),
              clientConfig,
            )(ec, sequencer)

          _ <- Runner.run(
            dar,
            triggerId,
            client,
            config.timeProviderType.getOrElse(RunnerConfig.DefaultTimeProviderType),
            applicationId,
            config.ledgerParty)
        } yield ()

        flow.onComplete(_ => system.terminate())

        Await.result(flow, Duration.Inf)
      }
    }
  }
}
