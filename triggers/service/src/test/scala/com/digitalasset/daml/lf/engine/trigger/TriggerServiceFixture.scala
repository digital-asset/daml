// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.io.File
import java.time.Duration

import akka.actor.ActorSystem
import akka.actor.typed.{ActorSystem => TypedActorSystem}
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxServer
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port

import scala.concurrent.{ExecutionContext, Future}

object TriggerServiceFixture {

  def withTriggerService[A](
      testName: String,
      dars: List[File],
      dar: Option[Dar[(PackageId, Package)]],
  )(testFn: (Uri, LedgerClient) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)
    val ledgerF = for {
      ledger <- Future(new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId), mat))
      port <- ledger.portF
    } yield (ledger, port.value)

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost("localhost", ledgerPort, clientConfig(applicationId))
    } yield client

    val serviceF: Future[(ServerBinding, TypedActorSystem[Server.Message])] = for {
      (_, ledgerPort) <- ledgerF
      ledgerConfig = LedgerConfig(
        "localhost",
        ledgerPort,
        TimeProviderType.Static,
        Duration.ofSeconds(30))
      service <- ServiceMain.startServer(
        "localhost",
        0,
        ledgerConfig,
        ServiceConfig.DefaultMaxInboundMessageSize,
        dar)
    } yield service

    val fa: Future[A] = for {
      client <- clientF
      binding <- serviceF
      uri = Uri.from(scheme = "http", host = "localhost", port = binding._1.localAddress.getPort)
      a <- testFn(uri, client)
    } yield a

    fa.onComplete { _ =>
      serviceF.foreach({ case (_, system) => system ! Server.Stop })
      ledgerF.foreach(_._1.close())
    }

    fa
  }

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId,
      authService: Option[AuthService] = None
  ): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.Static),
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
      authService = authService
    )

  private def clientConfig[A](
      applicationId: ApplicationId,
      token: Option[String] = None): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None,
      token = token
    )
}
