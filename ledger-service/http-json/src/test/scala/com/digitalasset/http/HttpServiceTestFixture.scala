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
import scala.util.Try

object HttpServiceTestFixture {

  private final case class Error(message: String)

  def withHttpService[A](dar: File, testName: String)(testFn: Uri => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)
    val ledgerPortT: Try[Int] = findOpenPort()
    val httpPortT: Try[Int] = findOpenPort()

    val ledgerF: Future[SandboxServer] = for {
      ledgerPort <- toFuture(ledgerPortT)
      ledger <- Future(SandboxServer(ledgerConfig(ledgerPort, dar, ledgerId)))
    } yield ledger

    val httpServiceF: Future[ServerBinding] = for {
      _ <- ledgerF
      ledgerPort <- toFuture(ledgerPortT)
      httpPort <- toFuture(httpPortT)
      httpService <- stripLeft(HttpService.start("localhost", ledgerPort, applicationId, httpPort))
    } yield httpService

    val fa: Future[A] = for {
      _ <- httpServiceF
      httpPort <- toFuture(httpPortT)
      uri = Uri.from(scheme = "http", host = "localhost", port = httpPort)
      a <- testFn(uri)
    } yield a

    fa.onComplete { _ =>
      ledgerF.foreach(_.close())
      httpServiceF.foreach(_.unbind())
    }

    fa
  }

  private def stripLeft(fa: Future[HttpService.Error \/ ServerBinding])(
      implicit ec: ExecutionContext): Future[ServerBinding] =
    fa.flatMap {
      case -\/(e) =>
        Future.failed(new IllegalStateException(s"Cannot start HTTP Service: ${e.message}"))
      case \/-(a) =>
        Future.successful(a)
    }

  private def ledgerConfig(ledgerPort: Int, dar: File, ledgerId: LedgerId): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = List(dar),
      timeProviderType = TimeProviderType.WallClock,
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
    )

}
