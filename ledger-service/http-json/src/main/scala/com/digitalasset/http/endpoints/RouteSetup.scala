package com.daml.http
package endpoints

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{
  Authorization,
  ModeledCustomHeader,
  ModeledCustomHeaderCompanion,
  OAuth2BearerToken,
  `X-Forwarded-Proto`,
}
import akka.stream.Materializer
import com.daml.http.EndpointsCompanion._
import com.daml.scalautil.Statement.discard
import com.daml.http.json._
import com.daml.http.util.FutureUtil.{either, eitherT}
import com.daml.http.util.Logging.{InstanceUUID, RequestID}
import com.daml.jwt.domain.Jwt
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.{-\/, \/, \/-}
import spray.json._

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}

class RouteSetup(
    allowNonHttps: Boolean,
    maxTimeToCollectRequest: FiniteDuration,
)(implicit ec: ExecutionContext, mat: Materializer) {
  import RouteSetup._
  import Endpoints.ET
  import util.ErrorOps._

  private[http] def proxyWithCommand[A: JsonReader, R](
      fn: (Jwt, A) => Future[Error \/ R]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[R] =
    for {
      t2 <- inputJsVal(req): ET[(Jwt, JsValue)]
      (jwt, reqBody) = t2
      a <- either(SprayJson.decode[A](reqBody).liftErr(InvalidUserInput)): ET[A]
      b <- eitherT(handleFutureEitherFailure(fn(jwt, a))): ET[R]
    } yield b

  private[http] def proxyWithCommandET[A: JsonReader, R](
      fn: (Jwt, A) => ET[R]
  )(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[R] = proxyWithCommand((jwt, a: A) => fn(jwt, a).run)(req)

  private[http] def input(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Future[Unauthorized \/ (Jwt, String)] = {
    findJwt(req) match {
      case e @ -\/(_) =>
        discard { req.entity.discardBytes(mat) }
        Future.successful(e)
      case \/-(j) =>
        data(req.entity).map(d => \/-((j, d)))
    }
  }

  private[http] def data(entity: RequestEntity): Future[String] =
    entity.toStrict(maxTimeToCollectRequest).map(_.data.utf8String)

  private[http] def inputJsVal(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): ET[(Jwt, JsValue)] =
    for {
      t2 <- eitherT(input(req)): ET[(Jwt, String)]
      jsVal <- either(SprayJson.parse(t2._2).liftErr(InvalidUserInput)): ET[JsValue]
    } yield (t2._1, jsVal)

  private[http] def findJwt(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Unauthorized \/ Jwt =
    ensureHttpsForwarded(req) flatMap { _ =>
      req.headers
        .collectFirst { case Authorization(OAuth2BearerToken(token)) =>
          Jwt(token)
        }
        .toRightDisjunction(
          Unauthorized("missing Authorization header with OAuth 2.0 Bearer Token")
        )
    }

  private[this] def ensureHttpsForwarded(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Unauthorized \/ Unit =
    if (allowNonHttps || isForwardedForHttps(req.headers)) \/-(())
    else {
      logger.warn(nonHttpsErrorMessage)
      \/-(())
    }
}

object RouteSetup {
  import Endpoints.IntoEndpointsError

  private val logger = ContextualizedLogger.get(getClass)

  private val nonHttpsErrorMessage =
    "missing HTTPS reverse-proxy request headers; for development launch with --allow-insecure-tokens"

  private[http] def logException(fromWhat: String)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID]
  ): Throwable PartialFunction Throwable = { case NonFatal(e) =>
    logger.error(s"$fromWhat failed", e)
    e
  }

  private[http] def handleFutureEitherFailure[A, B](fa: Future[A \/ B])(implicit
      ec: ExecutionContext,
      A: IntoEndpointsError[A],
      lc: LoggingContextOf[InstanceUUID with RequestID],
  ): Future[Error \/ B] =
    fa.map(_ leftMap A.run)
      .recover(logException("Future") andThen Error.fromThrowable andThen (-\/(_)))

  private def isForwardedForHttps(headers: Seq[HttpHeader]): Boolean =
    headers exists {
      case `X-Forwarded-Proto`(protocol) => protocol equalsIgnoreCase "https"
      // the whole "custom headers" thing in akka-http is a mishmash of
      // actually using the ModeledCustomHeaderCompanion stuff (which works)
      // and "just use ClassTag YOLO" (which won't work)
      case Forwarded(value) => Forwarded(value).proto contains "https"
      case _ => false
    }

  // avoid case class to avoid using the wrong unapply in isForwardedForHttps
  private[http] final class Forwarded(override val value: String)
      extends ModeledCustomHeader[Forwarded] {
    override def companion = Forwarded
    override def renderInRequests = true
    override def renderInResponses = false
    // per discussion https://github.com/digital-asset/daml/pull/5660#discussion_r412539107
    def proto: Option[String] =
      Forwarded.re findFirstMatchIn value map (_.group(1).toLowerCase)
  }

  private[http] object Forwarded extends ModeledCustomHeaderCompanion[Forwarded] {
    override val name = "Forwarded"
    override def parse(value: String) = Try(new Forwarded(value))
    private val re = raw"""(?i)proto\s*=\s*"?(https?)""".r
  }
}
