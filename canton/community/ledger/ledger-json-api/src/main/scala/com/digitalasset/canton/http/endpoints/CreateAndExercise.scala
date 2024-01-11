// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import org.apache.pekko.http.scaladsl.model.*
import com.daml.lf.value.Value as LfValue
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.domain.{
  CreateCommand,
  JwtPayloadTag,
  JwtWritePayload,
  SyncResponse,
}
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.util.toLedgerId
import com.digitalasset.canton.http.util.JwtParties.*
import com.daml.jwt.domain.Jwt
import com.daml.ledger.api.v1 as lav1
import lav1.value.{Record as ApiRecord, Value as ApiValue}
import scalaz.std.scalaFuture.*
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.*

import scala.concurrent.ExecutionContext
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed
import com.digitalasset.canton.http.domain.ContractTypeId.Template
import com.digitalasset.canton.http.{CommandService, ContractsService, domain}
import com.digitalasset.canton.http.metrics.HttpApiMetrics

private[http] final class CreateAndExercise(
    routeSetup: RouteSetup,
    decoder: DomainJsonDecoder,
    commandService: CommandService,
    contractsService: ContractsService,
)(implicit ec: ExecutionContext) {
  import CreateAndExercise.*
  import routeSetup.*, RouteSetup.*
  import com.digitalasset.canton.http.json.JsonProtocol.*
  import com.digitalasset.canton.http.util.ErrorOps.*

  def create(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            CreateCommand[ApiRecord, Template.RequiredPkg]
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())

        response <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd)),
          )
        ): ET[domain.CreateCommandResponse[ApiValue]]
      } yield response
    }

  def exercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.ExerciseCommand.RequiredPkg[LfValue, domain.ContractLocator[LfValue]]
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())
        resolvedRef <- resolveReference(jwt, jwtPayload, cmd.meta, cmd.reference)

        apiArg <- either(lfValueToApiValue(cmd.argument)): ET[ApiValue]

        resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef, meta = cmd.meta)

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.exercise(jwt, jwtPayload, resolvedCmd)
            ),
          )
        ): ET[domain.ExerciseResponse[ApiValue]]

      } yield resp
    }

  def createAndExercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateAndExerciseCommand(reqBody, jwt, toLedgerId(jwtPayload.ledgerId))
            .liftErr(InvalidUserInput): ET[
            domain.CreateAndExerciseCommand.LAVResolved
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
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
      meta: Option[domain.CommandMeta.IgnoreDisclosed],
      reference: domain.ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[domain.ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(
        jwt,
        resolveRefParties(meta, jwtPayload),
        reference,
        toLedgerId(jwtPayload.ledgerId),
      )
      .flatMap {
        case -\/((tpId, key)) => EitherT.either(lfValueToApiValue(key).map(k => -\/(tpId -> k)))
        case a @ \/-((_, _)) => EitherT.pure(a)
      }
}

object CreateAndExercise {
  import com.digitalasset.canton.http.util.ErrorOps.*

  private def lfValueToApiValue(a: LfValue): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a).liftErr(ServerError.fromMsg)
}
