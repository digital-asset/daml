// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import com.daml.lf.value.Value as LfValue
import com.digitalasset.canton.http.ContractsService.SearchResult
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.Endpoints.{ET, IntoEndpointsError}
import com.digitalasset.canton.http.domain.JwtPayload
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.JwtParties.*
import com.daml.jwt.domain.Jwt
import com.daml.logging.LoggingContextOf.withEnrichedLoggingContext
import scalaz.std.scalaFuture.*
import scalaz.syntax.std.option.*
import scalaz.syntax.traverse.*
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.*
import com.digitalasset.canton.http.json

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.LoggingContextOf
import com.digitalasset.canton.http.{ContractsService, domain}
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.NoTracing

private[http] final class ContractList(
    routeSetup: RouteSetup,
    decoder: DomainJsonDecoder,
    contractsService: ContractsService,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with NoTracing {
  import ContractList.*
  import routeSetup.*, RouteSetup.*
  import com.digitalasset.canton.http.json.JsonProtocol.*
  import com.digitalasset.canton.http.util.ErrorOps.*

  def fetch(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] = {
    for {
      parseAndDecodeTimer <- getParseAndDecodeTimerCtx()
      input <- inputJsValAndJwtPayload(req): ET[(Jwt, JwtPayload, JsValue)]

      (jwt, jwtPayload, reqBody) = input

      jsVal <- withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
        logger.debug(s"/v1/fetch reqBody: $reqBody, ${lc.makeString}")
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
                    .decodeContractLocatorKey(_, jwt)
                    .liftErr(InvalidUserInput)
                )
              ): ET[domain.FetchRequest[LfValue]]
          _ <- EitherT.pure(parseAndDecodeTimer.stop())
          _ = logger.debug(s"/v1/fetch fr: $fr, ${lc.makeString}")

          _ <- either(ensureReadAsAllowedByJwt(fr.readAs, jwtPayload))
          ac <- contractsService.lookup(jwt, jwtPayload, fr)

          jsVal <- either(
            ac.cata(x => toJsValue(x), \/-(JsNull))
          ): ET[JsValue]
        } yield jsVal
      }

    } yield domain.OkResponse(jsVal)
  }

  def retrieveAll(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): Future[Error \/ SearchResult[Error \/ JsValue]] = for {
    parseAndDecodeTimer <- Future(
      metrics.incomingJsonParsingAndValidationTimer.startAsync()
    )
    res <- inputAndJwtPayload[JwtPayload](req).run.map {
      _.map { case (jwt, jwtPayload, _) =>
        parseAndDecodeTimer.stop()
        withJwtPayloadLoggingContext(jwtPayload) { implicit lc =>
          val result: SearchResult[
            ContractsService.Error \/ domain.ActiveContract.ResolvedCtTyId[LfValue]
          ] =
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
      metrics: HttpApiMetrics,
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
          logger.debug(s"Processing a query request, ${lc.makeString}")
          contractsService
            .search(jwt, jwtPayload, cmd)
            .map(
              domain.SyncResponse.covariant.map(_)(
                _.via(handleSourceFailure)
                  .map(_.flatMap(toJsValue[domain.ActiveContract.ResolvedCtTyId[JsValue]](_)))
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
  import json.JsonProtocol.*
  import com.digitalasset.canton.http.util.ErrorOps.*

  private def lfValueToJsValue(a: LfValue): Error \/ JsValue =
    \/.attempt(LfValueCodec.apiValueToJsValue(a))(identity).liftErr(ServerError.fromMsg)

  private def lfAcToJsValue(a: domain.ActiveContract.ResolvedCtTyId[LfValue]): Error \/ JsValue = {
    for {
      b <- a.traverse(lfValueToJsValue): Error \/ domain.ActiveContract.ResolvedCtTyId[JsValue]
      c <- toJsValue(b)
    } yield c
  }

  private def toJsValue[A: JsonWriter](a: A): Error \/ JsValue = {
    SprayJson.encode(a).liftErr(ServerError.fromMsg)
  }
}
