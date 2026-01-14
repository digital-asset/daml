// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.auth.{AuthInterceptor, AuthServiceWildcard}
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, v2Endpoint}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError.*
import com.digitalasset.canton.http.util.GrpcHttpErrorCodes
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.{BaseTest, HasExecutionContext, UniquePortGenerator}
import io.circe
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.grpc.{Status, StatusRuntimeException}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.http.scaladsl.server.Route
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import sttp.model.StatusCode
import sttp.tapir.Endpoint
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.jsonBody
import sttp.tapir.server.pekkohttp.PekkoHttpServerInterpreter

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

class JsonErrorHandlingTest
    extends AsyncWordSpec
    with Matchers
    with EitherValues
    with PekkoBeforeAndAfterAll
    with BaseTest
    with HasExecutionContext {

  /** Test uses Mocked Service added to simulate "programmers" errors - that are unlikely and
    * probably impossible to happen under normal circumstances. The purpose of a test is to ensure
    * that during development the errors from gRPC layers are still correctly returned, even if they
    * are results of buggy/unfinished code.
    */

  private val fakeApiLogger = new ApiRequestLogger(
    config = ApiLoggingConfig(),
    loggerFactory = loggerFactory,
  )

  val service = new TestJsService(
    requestLogger = fakeApiLogger,
    loggerFactory = loggerFactory,
  )

  private val port = UniquePortGenerator.next.unwrap

  private val LEDGER_API_INTERNAL_ERROR_CODE = "LEDGER_API_INTERNAL_ERROR"

  private lazy val host = "localhost"
  private lazy val route: Route = PekkoHttpServerInterpreter().toRoute(service.endpoints())

  @volatile private var binding: Http.ServerBinding = _

  override def beforeAll(): Unit = {
    binding = Http()(system).newServerAt(host, port).bind(route).futureValue
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    binding.unbind().futureValue
    super.afterAll()
  }

  "Endpoints error handling" should {

    "handle mismatched error Status" in {
      for {
        response <- call("/v2/test/inconsistent-codes-error", port)
        _ = response.status.intValue() should be(StatusCode.InternalServerError.code)
        body <- extractBodyFromResponse(response)
      } yield {
        val error: JsCantonError = body.value
        error.grpcCodeValue should be(Some(Status.INTERNAL.getCode.value()))
        error.cause should be(
          "INTERNAL: outer error"
        )
      }
    }

    "handle runtime exception" in {
      for {
        response <- loggerFactory.assertLogs(
          call("/v2/test/runtime-exception", port),
          _.errorMessage should include(
            "LEDGER_API_INTERNAL_ERROR(4,0): unexpected situation"
          ),
        )
        _ = response.status.intValue() should be(StatusCode.InternalServerError.code)
        body <- extractBodyFromResponse(response)
      } yield {
        val error: JsCantonError = body.value
        error.code should be(LEDGER_API_INTERNAL_ERROR_CODE)
        error.grpcCodeValue should be(Some(Status.INTERNAL.getCode.value()))
        error.cause should be(
          "unexpected situation"
        )
      }
    }

    "handle status runtime exception without description" in {
      for {
        response <- call("/v2/test/status-runtime-exception", port)
        _ = response.status.intValue() should be(StatusCode.InternalServerError.code)
        body <- extractBodyFromResponse(response)
      } yield {
        val error: JsCantonError = body.value
        error.grpcCodeValue should be(Some(Status.INTERNAL.getCode.value()))
        error.cause should be(
          "INTERNAL"
        )
        error.code should be("Status description not available")
      }
    }

    "map GRPC Status Code to HTTP Status Code correctly" in {
      import com.google.rpc.Code
      import GrpcHttpErrorCodes.`gRPC status as pekko http` as GrpcToHttpConverter
      forAll(Code.values().filter(_ != Code.UNRECOGNIZED).toList) { grpcCode =>
        val url = s"/v2/test/parametrized-status-runtime-exception?code=${grpcCode.getNumber}"
        for {
          response <- call(url, port)
          _ = response.status.intValue() should be(
            GrpcToHttpConverter(grpcCode).asPekkoHttp.intValue
          )
          body <- extractBodyFromResponse(response)
        } yield {
          val error: JsCantonError = body.value
          error.grpcCodeValue should be(Some(grpcCode.getNumber))
        }
      }
    }
  }

  private def call(endpoint: String, port: Int): Future[HttpResponse] = {
    val request = HttpRequest(uri = s"http://localhost:$port$endpoint")
    Http()(system).singleRequest(request)
  }

  private def extractBodyFromResponse(
      response: HttpResponse
  ): Future[Either[circe.Error, JsCantonError]] =
    response.entity
      .toStrict(30.seconds)
      .map(_.data.utf8String)
      .map(io.circe.parser.decode[JsCantonError])
}

final case class NoRealData(any: String)

class TestJsService(
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends Endpoints {
  implicit val config: Configuration = io.circe.generic.extras.Configuration.default.withDefaults
  implicit val codec: Codec[NoRealData] = deriveRelaxedCodec

  implicit val authInterceptor: AuthInterceptor =
    new AuthInterceptor(
      List(AuthServiceWildcard),
      loggerFactory,
      executionContext,
    )

  private lazy val test = v2Endpoint.in(sttp.tapir.stringToPath("test"))

  def endpoints() = List(
    // Inconsistent status codes,
    // very unlikely to happen, but originally this situation was not handled well
    withServerLogic(
      inconsistentStatusCodesEndpoint,
      (_: CallerContext) => { (_: TracedInput[Unit]) =>
        val statusProto = com.google.rpc.Status
          .newBuilder()
          .setCode(2)
          .setMessage("error from trailers")
          .build()
        val mismatchedTrailers =
          io.grpc.protobuf.StatusProto.toStatusRuntimeException(statusProto).getTrailers
        throw new StatusRuntimeException(
          Status.fromCodeValue(13).withDescription("outer error"),
          mismatchedTrailers,
        )
      },
    ),
    withServerLogic(
      statusRuntimeExceptionEndpoint,
      (_: CallerContext) => { (_: TracedInput[Unit]) =>
        throw new StatusRuntimeException(Status.INTERNAL)
      },
    ),
    withServerLogic(
      runtimeExceptionEndpoint,
      (_: CallerContext) => { (_: TracedInput[Unit]) =>
        throw new RuntimeException("unexpected situation")
      },
    ),
    withServerLogic(
      parametrizedStatusRuntimeExceptionEndpoint,
      (_: CallerContext) => { (codeInput: TracedInput[Int]) =>
        throw new StatusRuntimeException(Status.fromCodeValue(codeInput.in))
      },
    ),
  )

  val inconsistentStatusCodesEndpoint
      : Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), NoRealData, Any] =
    test.get
      .in(sttp.tapir.stringToPath("inconsistent-codes-error"))
      .out(jsonBody[NoRealData])

  val runtimeExceptionEndpoint
      : Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), NoRealData, Any] =
    test.get
      .in(sttp.tapir.stringToPath("runtime-exception"))
      .out(jsonBody[NoRealData])

  val statusRuntimeExceptionEndpoint
      : Endpoint[CallerContext, Unit, (StatusCode, JsCantonError), NoRealData, Any] =
    test.get
      .in(sttp.tapir.stringToPath("status-runtime-exception"))
      .out(jsonBody[NoRealData])

  val parametrizedStatusRuntimeExceptionEndpoint
      : Endpoint[CallerContext, Int, (StatusCode, JsCantonError), NoRealData, Any] =
    test.get
      .in(sttp.tapir.stringToPath("parametrized-status-runtime-exception"))
      .in(
        sttp.tapir
          .query[Int]("code")
          .description("The gRPC status code to be converted to HTTP status code")
      )
      .out(jsonBody[NoRealData])
}
