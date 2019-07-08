// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.util.FutureUtil.toFuture
import com.digitalasset.http.util.TestUtil.findOpenPort
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import scalaz._

import scala.concurrent.{ExecutionContext, Future}

object HttpServiceTestFixture {

  def withHttpService[A](dar: File, testName: String)(testFn: Uri => Future[A])(
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
      httpService <- stripLeft(HttpService.start("localhost", ledgerPort, applicationId, httpPort))
    } yield (httpService, httpPort)

    val fa: Future[A] = for {
      (_, httpPort) <- httpServiceF
      uri = Uri.from(scheme = "http", host = "localhost", port = httpPort)
      a <- testFn(uri)
    } yield a

    fa.onComplete { _ =>
      ledgerF.foreach(_._1.close())
      httpServiceF.foreach(_._1.unbind())
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

  private def stripLeft(fa: Future[HttpService.Error \/ ServerBinding])(
      implicit ec: ExecutionContext): Future[ServerBinding] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(s"Cannot start HTTP Service: ${e.message}"))
      case \/-(a) =>
        Future.successful(a)
    }
}
