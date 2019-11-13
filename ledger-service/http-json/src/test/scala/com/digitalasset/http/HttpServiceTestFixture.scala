// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.digitalasset.http.util.FutureUtil
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.IdentifierConverters.apiLedgerId
import com.digitalasset.http.util.TestUtil.findOpenPort
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.ledger.client.LedgerClient
import com.digitalasset.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import scalaz._

import scala.concurrent.duration.{DAYS, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

object HttpServiceTestFixture {

  private val doNotReloadPackages = FiniteDuration(100, DAYS)

  def withHttpService[A](dar: File, jdbcConfig: Option[JdbcConfig], testName: String)(
      testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val ledgerF: Future[(SandboxServer, Int)] = for {
      port <- toFuture(findOpenPort())
      ledger <- Future(SandboxServer(ledgerConfig(port, dar, ledgerId)))
    } yield (ledger, port)

    val httpServiceF: Future[(ServerBinding, Int)] = for {
      (_, ledgerPort) <- ledgerF
      httpPort <- toFuture(findOpenPort())
      httpService <- stripLeft(
        HttpService.start(
          "localhost",
          ledgerPort,
          applicationId,
          "localhost",
          httpPort,
          jdbcConfig,
          None,
          doNotReloadPackages))
    } yield (httpService, httpPort)

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost("localhost", ledgerPort, clientConfig(applicationId))
    } yield client

    val codecsF: Future[(DomainJsonEncoder, DomainJsonDecoder)] = for {
      client <- clientF
      codecs <- jsonCodecs(client)
    } yield codecs

    val fa: Future[A] = for {
      (_, httpPort) <- httpServiceF
      (encoder, decoder) <- codecsF
      uri = Uri.from(scheme = "http", host = "localhost", port = httpPort)
      a <- testFn(uri, encoder, decoder)
    } yield a

    fa.onComplete { _ =>
      ledgerF.foreach(_._1.close())
      httpServiceF.foreach(_._1.unbind())
    }

    fa
  }

  def withLedger[A](dar: File, testName: String)(testFn: LedgerClient => Future[A])(
      implicit aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val ledgerF: Future[(SandboxServer, Int)] = for {
      port <- toFuture(findOpenPort())
      ledger <- Future(SandboxServer(ledgerConfig(port, dar, ledgerId)))
    } yield (ledger, port)

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost("localhost", ledgerPort, clientConfig(applicationId))
    } yield client

    val fa: Future[A] = for {
      client <- clientF
      a <- testFn(client)
    } yield a

    fa.onComplete { _ =>
      ledgerF.foreach(_._1.close())
    }

    fa
  }

  private def ledgerConfig(ledgerPort: Int, dar: File, ledgerId: LedgerId): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = List(dar),
      timeProviderType = TimeProviderType.WallClock,
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
    )

  private def clientConfig[A](applicationId: ApplicationId): LedgerClientConfiguration =
    LedgerClientConfiguration(
      applicationId = ApplicationId.unwrap(applicationId),
      ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
      commandClient = CommandClientConfiguration.default,
      sslContext = None
    )

  def jsonCodecs(client: LedgerClient)(
      implicit ec: ExecutionContext): Future[(DomainJsonEncoder, DomainJsonDecoder)] = {
    val ledgerId = apiLedgerId(client.ledgerId)
    val packageService = new PackageService(
      HttpService.loadPackageStoreUpdates(client.packageClient))
    packageService
      .reload(ec)
      .flatMap(x => FutureUtil.toFuture(x))
      .map(_ => HttpService.buildJsonCodecs(ledgerId, packageService))
  }

  private def stripLeft(fa: Future[HttpService.Error \/ ServerBinding])(
      implicit ec: ExecutionContext): Future[ServerBinding] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(s"Cannot start HTTP Service: ${e.message}"))
      case \/-(a) =>
        Future.successful(a)
    }
}
