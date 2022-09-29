// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{Flow, Source}
import com.daml.lf.value.{Value => LfValue}
import ContractsService.SearchResult
import EndpointsCompanion._
import Endpoints.{ET, IntoEndpointsError}
import domain.JwtPayload
import json._
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import util.toLedgerId
import util.JwtParties._
import com.daml.jwt.domain.Jwt
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}
import com.daml.metrics.Metrics

private[http] final class ContractList(
    routeSetup: RouteSetup,
    decoder: DomainJsonDecoder,
    contractsService: ContractsService,
)(implicit ec: ExecutionContext) {
  import ContractList._
  import routeSetup._, RouteSetup._
  import json.JsonProtocol._
  import util.ErrorOps._

  def fetch(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    for {
      parseAndDecodeTimerCtx <- getParseAndDecodeTimerCtx()
      input <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = input

      jsVal <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        logger.debug(s"/v1/fetch reqBody: $reqBody")
        for {
          fr <-
            either(
              SprayJson
                .decode[domain.FetchRequest[JsValue]](reqBody)
                .liftErr[Error](InvalidUserInput)
            )
              .flatMap(
                _.traverseLocator(
                  decoder
                    .decodeContractLocatorKey(_, jwt, toLedgerId(jwtPayload.ledgerId))
                    .liftErr(InvalidUserInput)
                )
              ): ET[domain.FetchRequest[LfValue]]
          _ <- EitherT.pure(parseAndDecodeTimerCtx.close())
          _ = logger.debug(s"/v1/fetch fr: $fr")

          _ <- either(ensureReadAsAllowedByJwt(fr.readAs, jwtPayload))
          ac <- eitherT(
            handleFutureFailure(contractsService.lookup(jwt, jwtPayload, fr))
          ): ET[Option[domain.ActiveContract[JsValue]]]

          jsVal <- either(
            ac.cata(x => toJsValue(x), \/-(JsNull))
          ): ET[JsValue]
        } yield jsVal
      }

    } yield domain.OkResponse(jsVal)

  def retrieveAll(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = for {
    parseAndDecodeTimerCtx <- Future(
      metrics.daml.HttpJsonApi.incomingJsonParsingAndValidationTimer.metric.time()
    )
    res <- inputAndJwtPayload[JwtPayload](req).run.map {
      _.map { case (jwt, jwtPayload, _) =>
        parseAndDecodeTimerCtx.close()
        withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
          val result: SearchResult[ContractsService.Error \/ domain.ActiveContract[LfValue]] =
            contractsService.retrieveAll(jwt, jwtPayload)

          domain.SyncResponse.covariant.map(result) { source =>
            source
              .via(handleSourceFailure)
              .map(_.flatMap(lfAcToJsValue)): Source[Error \/ JsValue, NotUsed]
          }
        }
      }
    }
  } yield res

  def query(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = {
    for {
      it <- inputAndJwtPayload[JwtPayload](req).leftMap(identity[Error])
      (jwt, jwtPayload, reqBody) = it
      res <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        val res = for {
          cmd <- SprayJson
            .decode[domain.GetActiveContractsRequest](reqBody)
            .liftErr[Error](InvalidUserInput)
          _ <- ensureReadAsAllowedByJwt(cmd.readAs, jwtPayload)
        } yield withEnrichedLoggingContext(
          LoggingContextOf.label[domain.GetActiveContractsRequest],
          "cmd" -> cmd.toString,
        ).run { implicit lc =>
          logger.debug("Processing a query request")
          contractsService
            .search(jwt, jwtPayload, cmd)
            .map(
              domain.SyncResponse.covariant.map(_)(
                _.via(handleSourceFailure)
                  .map(_.flatMap(toJsValue[domain.ActiveContract[JsValue]](_)))
              )
            )
        }
        eitherT(res.sequence)
      }
    } yield res
  }.run

  private def handleSourceFailure[E, A](implicit
      E: IntoEndpointsError[E]
  ): Flow[E \/ A, Error \/ A, NotUsed] =
    Flow
      .fromFunction((_: E \/ A).leftMap(E.run))
      .recover(Error.fromThrowable andThen (-\/(_)))

}

private[endpoints] object ContractList {
  import json.JsonProtocol._
  import util.ErrorOps._

  private val logger = ContextualizedLogger.get(getClass)

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(identity).liftErr(ServerError.fromMsg)

  private def lfAcToJsValue(a: domain.ActiveContract[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract[JsValue]
      c <- toJsValue(b)
    } yield c
  }

  private def toJsValue[A: JsonWriter](a: A): Error \/ JsValue = {
    SprayJson.encode(a).liftErr(ServerError.fromMsg)
  }
}
