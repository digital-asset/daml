// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.ledger.api.v2.contract_service
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{JsCantonError, JsEvent}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import sttp.tapir.AnyEndpoint
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class JsContractService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    val authInterceptor: AuthInterceptor,
) extends Endpoints
    with NamedLogging {

  private def contractServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): contract_service.ContractServiceGrpc.ContractServiceStub =
    ledgerClient.serviceClient(contract_service.ContractServiceGrpc.stub, token)

  def endpoints() = List(
    withServerLogic(
      JsContractService.getContractEndpoint,
      getContract,
    )
  )

  private def getContract(
      caller: CallerContext
  ): TracedInput[contract_service.GetContractRequest] => Future[
    Either[JsCantonError, JsContractService.GetContractResponse]
  ] = { req =>
    implicit val tc: TraceContext = caller.traceContext()
    contractServiceClient(caller.token())
      .getContract(req.in)
      .flatMap(protocolConverters.GetContractResponse.toJson)
      .resultToRight
  }
}

object JsContractService extends DocumentationEndpoints {
  import Endpoints.*
  import JsContractServiceCodecs.*

  private lazy val contracts = v2Endpoint.in(sttp.tapir.stringToPath("contracts"))

  val getContractEndpoint = contracts.post
    .in(sttp.tapir.stringToPath("contract-by-id"))
    .in(jsonBody[contract_service.GetContractRequest])
    .out(jsonBody[JsContractService.GetContractResponse])
    .description("""Looking up contract data by contract ID.
        |This endpoint is experimental / alpha, therefore no backwards compatibility is guaranteed.
        |This endpoint must not be used to look up contracts which entered the participant via party replication
        |or repair service.
        |""".stripMargin)

  override def documentation: Seq[AnyEndpoint] = List(
    getContractEndpoint
  )

  final case class GetContractResponse(createdEvent: JsEvent.CreatedEvent)
}

object JsContractServiceCodecs {
  import JsSchema.config

  implicit val jsGetContractResponseRW: Codec[JsContractService.GetContractResponse] =
    deriveConfiguredCodec
  implicit val getContractRequestRW: Codec[contract_service.GetContractRequest] = deriveRelaxedCodec
}
