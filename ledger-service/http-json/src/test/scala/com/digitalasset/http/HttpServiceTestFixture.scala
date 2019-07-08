package com.digitalasset.http

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.stream.Materializer
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.http.util.FutureUtil
import com.digitalasset.http.util.FutureUtil.{liftET, toFuture}
import com.digitalasset.http.util.TestUtil.findOpenPort
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.services.time.TimeProviderType
import scalaz.EitherT.{eitherT, rightT}
import scalaz._
import scalaz.std.scalaFuture._

import scala.concurrent.{ExecutionContext, Future}
import scala.{util => u}

object HttpServiceTestFixture {

  private final case class Error(message: String)

  def withHttpService[A](dar: File, testName: String)(testFn: Uri => Future[A])(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext): Future[A] = {

    val ledgerId = LedgerId(testName)
    val applicationId = ApplicationId(testName)

    val ledgerF = for {
      ledgerPort <- toFuture(findOpenPort())
      ledger <- Future(SandboxServer(ledgerConfig(ledgerPort, dar, ledgerId)))
    } yield ledger

    val httpServiceF = for {
      httpPort <- toFuture(findOpenPort())
      httpService <- HttpService.start("localhost", ledgerPort, applicationId, httpPort))
    }

    val f: EitherT[Future, Error, A] = for {
      ledgerPort <- hoistTry[Int](findOpenPort())
      httpPort <- hoistTry[Int](findOpenPort())
      ledger <- liftET(Future(SandboxServer(ledgerConfig(ledgerPort, dar, ledgerId))))
      httpService <- eitherT(
        HttpService
          .start("localhost", ledgerPort, applicationId, httpPort))
        .leftMap(e => Error(e.message))
      uri = Uri.from(scheme = "http", host = "localhost", port = httpPort)
      a <- liftET(testFn(uri))
      _ <- rightT(httpService.unbind())
      _ <- rightT(Future(ledger.close()))
    } yield a

    f.run.flatMap {
      case \/-(a) => Future.successful(a)
      case -\/(e) => Future.failed(new IllegalStateException(e.message))
    }
  }

  private def hoistTry[A](t: u.Try[A]): EitherT[Future, Error, A] =
    t.fold(
      e => eitherT[Future, Error, A](Future.successful(\/.left(Error(e.getMessage)))),
      a => eitherT[Future, Error, A](Future.successful(\/.right(a)))
    )

  private def ledgerConfig(ledgerPort: Int, dar: File, ledgerId: LedgerId): SandboxConfig =
    SandboxConfig.default.copy(
      port = ledgerPort,
      damlPackages = List(dar),
      timeProviderType = TimeProviderType.WallClock,
      ledgerIdMode = LedgerIdMode.Static(ledgerId),
    )

}
