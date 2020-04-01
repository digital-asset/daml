// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.http.dbbackend.ContractDao
import com.daml.http.json.{DomainJsonDecoder, DomainJsonEncoder}
import com.daml.http.util.FutureUtil
import com.daml.http.util.IdentifierConverters.apiLedgerId
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
import scalaz._
import scalaz.std.option._
import scalaz.std.scalaFuture._
import scalaz.syntax.traverse._

import scala.concurrent.duration.{DAYS, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

object HttpServiceTestFixture {

  private val doNotReloadPackages = FiniteDuration(100, DAYS)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def withHttpService[A](
      testName: String,
      dars: List[File],
      jdbcConfig: Option[JdbcConfig],
      staticContentConfig: Option[StaticContentConfig]
  )(testFn: (Uri, DomainJsonEncoder, DomainJsonDecoder, LedgerClient) => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val contractDaoF: Future[Option[ContractDao]] = jdbcConfig.map(c => initializeDb(c)).sequence

    val ledgerF = for {
      ledger <- Future(new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId), mat))
      port <- ledger.portF
    } yield (ledger, port)

    val httpServiceF: Future[ServerBinding] = for {
      (_, ledgerPort) <- ledgerF
      contractDao <- contractDaoF
      httpService <- stripLeft(
        HttpService.start(
          "localhost",
          ledgerPort.value,
          applicationId,
          "localhost",
          0,
          Some(Config.DefaultWsConfig),
          None,
          contractDao,
          staticContentConfig,
          doNotReloadPackages))
    } yield httpService

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost("localhost", ledgerPort.value, clientConfig(applicationId))
    } yield client

    val codecsF: Future[(DomainJsonEncoder, DomainJsonDecoder)] = for {
      client <- clientF
      codecs <- jsonCodecs(client)
    } yield codecs

    val fa: Future[A] = for {
      httpService <- httpServiceF
      address = httpService.localAddress
      uri = Uri.from(scheme = "http", host = address.getHostName, port = address.getPort)
      (encoder, decoder) <- codecsF
      client <- clientF
      a <- testFn(uri, encoder, decoder, client)
    } yield a

    fa.transformWith { ta =>
      Future
        .sequence(
          Seq(
            ledgerF.map(_._1.close()),
            httpServiceF.flatMap(_.unbind()),
          ) map (_ fallbackTo Future.successful(())))
        .transform(_ => ta)
    }
  }

  def withLedger[A](
      dars: List[File],
      testName: String,
      token: Option[String] = None,
      authService: Option[AuthService] = None)(testFn: LedgerClient => Future[A])(
      implicit mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val ledgerF = for {
      ledger <- Future(
        new SandboxServer(ledgerConfig(Port.Dynamic, dars, ledgerId, authService), mat))
      port <- ledger.portF
    } yield (ledger, port)

    val clientF: Future[LedgerClient] = for {
      (_, ledgerPort) <- ledgerF
      client <- LedgerClient.singleHost(
        "localhost",
        ledgerPort.value,
        clientConfig(applicationId, token))
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

  private def ledgerConfig(
      ledgerPort: Port,
      dars: List[File],
      ledgerId: LedgerId,
      authService: Option[AuthService] = None
  ): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = dars,
      timeProviderType = Some(TimeProviderType.WallClock),
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

  def jsonCodecs(client: LedgerClient)(
      implicit ec: ExecutionContext): Future[(DomainJsonEncoder, DomainJsonDecoder)] = {
    val ledgerId = apiLedgerId(client.ledgerId)
    val packageService = new PackageService(
      HttpService.loadPackageStoreUpdates(client.packageClient, holderM = None))
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

  private def initializeDb(c: JdbcConfig)(implicit ec: ExecutionContext): Future[ContractDao] =
    for {
      dao <- Future(ContractDao(c.driver, c.url, c.user, c.password))
      _ <- dao.transact(ContractDao.initialize(dao.logHandler)).unsafeToFuture(): Future[Unit]
    } yield dao
}
