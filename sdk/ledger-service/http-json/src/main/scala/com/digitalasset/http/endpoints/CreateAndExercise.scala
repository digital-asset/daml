// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package endpoints

import akka.http.scaladsl.model._
import com.daml.lf.value.{Value => LfValue}
import EndpointsCompanion._
import Endpoints.ET
import domain.{JwtPayloadTag, JwtWritePayload, TemplateId}
import json._
import util.FutureUtil.{either, eitherT}
import util.Logging.{InstanceUUID, RequestID}
import util.toLedgerId
import util.JwtParties._
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.{v1 => lav1}
import lav1.value.{Value => ApiValue, Record => ApiRecord}
import scalaz.std.scalaFuture._
import scalaz.syntax.std.option._
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import com.daml.logging.LoggingContextOf
import com.daml.metrics.{Metrics, Timed}

private[http] final class CreateAndExercise(
    routeSetup: RouteSetup,
    decoder: DomainJsonDecoder,
    commandService: CommandService,
    contractsService: ContractsService,
)(implicit ec: ExecutionContext) {
  import CreateAndExercise._
  import routeSetup._, RouteSetup._
  import json.JsonProtocol._
  import util.ErrorOps._

  def create(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[domain.CreateCommand[ApiRecord, TemplateId.RequiredPkg]]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())

        ac <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd)),
          )
        ): ET[domain.ActiveContract[ApiValue]]
      } yield ac
    }

  def exercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.ExerciseCommand[LfValue, domain.ContractLocator[LfValue]]
          ]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())
        resolvedRef <- eitherT(
          resolveReference(jwt, jwtPayload, cmd.meta, cmd.reference)
        ): ET[domain.ResolvedContractRef[ApiValue]]

        apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

        resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef)

        resp <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.exercise(jwt, jwtPayload, resolvedCmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]

      } yield resp
    }

  def createAndExercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: Metrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimerCtx) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateAndExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.CreateAndExerciseCommand[ApiRecord, ApiValue, TemplateId.RequiredPkg]
          ]
        _ <- EitherT.pure(parseAndDecodeTimerCtx.close())

        resp <- eitherT(
          Timed.future(
            metrics.daml.HttpJsonApi.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.createAndExercise(jwt, jwtPayload, cmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]
      } yield resp
    }

  private def resolveReference(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      meta: Option[domain.CommandMeta],
      reference: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: Metrics,
  ): Future[Error \/ domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(
        jwt,
        resolveRefParties(meta, jwtPayload),
        reference,
        toLedgerId(jwtPayload.ledgerId),
      )
      .map { o: Option[domain.ResolvedContractRef[LfValue]] =>
        val a: Error \/ domain.ResolvedContractRef[LfValue] =
          o.toRightDisjunction(InvalidUserInput(ErrorMessages.cannotResolveTemplateId(reference)))
        a.flatMap {
          case -\/((tpId, key)) => lfValueToApiValue(key).map(k => -\/((tpId, k)))
          case a @ \/-((_, _)) => \/-(a)
        }
      }
}

private[http] object CreateAndExercise {
  import util.ErrorOps._

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).liftErr(ServerError.fromMsg)
}
