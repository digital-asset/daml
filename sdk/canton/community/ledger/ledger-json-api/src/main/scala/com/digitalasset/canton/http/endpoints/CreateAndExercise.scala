// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.endpoints

import com.daml.jwt.Jwt
import com.daml.ledger.api.v2 as lav2
import com.daml.logging.LoggingContextOf
import com.daml.metrics.Timed
import com.digitalasset.canton.http.ContractTypeId.Template
import com.digitalasset.canton.http.Endpoints.ET
import com.digitalasset.canton.http.EndpointsCompanion.*
import com.digitalasset.canton.http.json.*
import com.digitalasset.canton.http.metrics.HttpApiMetrics
import com.digitalasset.canton.http.util.FutureUtil.{either, eitherT}
import com.digitalasset.canton.http.util.JwtParties.*
import com.digitalasset.canton.http.util.Logging.{InstanceUUID, RequestID}
import com.digitalasset.canton.http.{
  CommandMeta,
  CommandService,
  ContractLocator,
  ContractsService,
  CreateAndExerciseCommand,
  CreateCommand,
  CreateCommandResponse,
  ExerciseCommand,
  ExerciseResponse,
  JwtPayloadTag,
  JwtWritePayload,
  ResolvedContractRef,
  SyncResponse,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.Value as LfValue
import org.apache.pekko.http.scaladsl.model.*
import scalaz.std.scalaFuture.*
import scalaz.{-\/, EitherT, \/, \/-}
import spray.json.*

import scala.concurrent.ExecutionContext

import lav2.value.{Record as ApiRecord, Value as ApiValue}

private[http] final class CreateAndExercise(
    routeSetup: RouteSetup,
    decoder: ApiJsonDecoder,
    commandService: CommandService,
    contractsService: ContractsService,
    resolvePackageName: Ref.PackageId => Ref.PackageName,
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
    handleCommand(req) {
      (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => implicit tc => implicit lc =>
        for {
          cmd <-
            decoder
              .decodeCreateCommand(reqBody, jwt)
              .liftErr(InvalidUserInput.apply): ET[
              CreateCommand[ApiRecord, Template.RequiredPkg]
            ]
          _ <- EitherT.pure(parseAndDecodeTimer.stop())

          response <- eitherT(
            Timed.future(
              metrics.commandSubmissionLedgerTimer,
              handleFutureEitherFailure(commandService.create(jwt, jwtPayload, cmd)),
            )
          ): ET[CreateCommandResponse[ApiValue]]
        } yield response
    }

  def exercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      ec: ExecutionContext,
      metrics: HttpApiMetrics,
  ): ET[SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => _ => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeExerciseCommand(reqBody, jwt)
            .liftErr(InvalidUserInput.apply): ET[
            ExerciseCommand.RequiredPkg[LfValue, ContractLocator[LfValue]]
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())
        resolvedRef <- resolveReference(jwt, jwtPayload, cmd.meta, cmd.reference)

        apiArg <- either(lfValueToApiValue(cmd.argument, resolvePackageName)): ET[ApiValue]

        resolvedCmd = cmd.copy(argument = apiArg, reference = resolvedRef, meta = cmd.meta)

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.exercise(jwt, jwtPayload, resolvedCmd)
            ),
          )
        ): ET[ExerciseResponse[ApiValue]]

      } yield resp
    }

  def createAndExercise(req: HttpRequest)(implicit
      lc: LoggingContextOf[InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[SyncResponse[JsValue]] =
    handleCommand(req) { (jwt, jwtPayload, reqBody, parseAndDecodeTimer) => _ => implicit lc =>
      for {
        cmd <-
          decoder
            .decodeCreateAndExerciseCommand(reqBody, jwt)
            .liftErr(InvalidUserInput.apply): ET[
            CreateAndExerciseCommand.LAVResolved
          ]
        _ <- EitherT.pure(parseAndDecodeTimer.stop())

        resp <- eitherT(
          Timed.future(
            metrics.commandSubmissionLedgerTimer,
            handleFutureEitherFailure(
              commandService.createAndExercise(jwt, jwtPayload, cmd)
            ),
          )
        ): ET[ExerciseResponse[ApiValue]]
      } yield resp
    }

  private def resolveReference(
      jwt: Jwt,
      jwtPayload: JwtWritePayload,
      meta: Option[CommandMeta.IgnoreDisclosed],
      reference: ContractLocator[LfValue],
  )(implicit
      lc: LoggingContextOf[JwtPayloadTag with InstanceUUID with RequestID],
      metrics: HttpApiMetrics,
  ): ET[ResolvedContractRef[ApiValue]] =
    contractsService
      .resolveContractReference(
        jwt,
        resolveRefParties(meta, jwtPayload),
        reference,
      )
      .flatMap {
        case -\/((tpId, key)) =>
          EitherT.either(lfValueToApiValue(key, resolvePackageName).map(k => -\/(tpId -> k)))
        case a @ \/-((_, _)) => EitherT.pure(a)
      }
}

object CreateAndExercise {
  import com.digitalasset.canton.http.util.ErrorOps.*

  private def lfValueToApiValue(
      a: LfValue,
      resolvePackageName: Ref.PackageId => Ref.PackageName,
  ): Error \/ ApiValue =
    JsValueToApiValueConverter.lfValueToApiValue(a, resolvePackageName).liftErr(ServerError.fromMsg)
}
