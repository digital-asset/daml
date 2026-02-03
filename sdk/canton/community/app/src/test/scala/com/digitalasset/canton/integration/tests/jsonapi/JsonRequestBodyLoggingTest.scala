// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.jsonapi

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.auth.{AuthInterceptor, AuthServiceWildcard}
import com.digitalasset.canton.config.ApiLoggingConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput, v2Endpoint}
import com.digitalasset.canton.http.json.v2.JsSchema.JsCantonError
import com.digitalasset.canton.http.json.v2.{Endpoints, RequestInterceptors}
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, UniquePortGenerator}
import io.circe.Codec
import io.circe.generic.extras.Configuration
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{
  ContentType,
  HttpCharsets,
  HttpEntity,
  HttpRequest,
  HttpResponse,
  MediaType,
  RequestEntity,
}
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import sttp.model.StatusCode
import sttp.tapir.Endpoint
import sttp.tapir.server.pekkohttp.{PekkoHttpServerInterpreter, PekkoHttpServerOptions}

import java.nio.charset.StandardCharsets
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class JsonRequestBodyLoggingTest
    extends AsyncWordSpec
    with Matchers
    with EitherValues
    with PekkoBeforeAndAfterAll
    with BaseTest
    with HasExecutionContext {

  // Test uses Mocked Service added to simulate request body logging

  private val bodyLength = 10

  private val messagePayloadEnabledApiConfig = ApiLoggingConfig(messagePayloads = true)
  private val messagePayloadDisabledApiConfig = ApiLoggingConfig()

  private val messagePayloadsEnabledLogger = new ApiRequestLogger(
    config = messagePayloadEnabledApiConfig,
    loggerFactory = loggerFactory,
  )
  private val messagePayloadsDisabledLogger = new ApiRequestLogger(
    config = messagePayloadDisabledApiConfig,
    loggerFactory = loggerFactory,
  )

  private val requestBodyLoggingInterceptors =
    new RequestInterceptors(messagePayloadsEnabledLogger, loggerFactory)(
      executionContext = executionContext,
      apiLoggingConfig = messagePayloadEnabledApiConfig,
    )
  private val requestBodyLoggingInterceptorsDisabled =
    new RequestInterceptors(messagePayloadsDisabledLogger, loggerFactory)(
      executionContext = executionContext,
      apiLoggingConfig = messagePayloadDisabledApiConfig,
    )

  private val route1Options: PekkoHttpServerOptions = PekkoHttpServerOptions.default
    .prependInterceptor(requestBodyLoggingInterceptors.loggingInterceptor())
  private val route2Options: PekkoHttpServerOptions = PekkoHttpServerOptions.default
    .prependInterceptor(requestBodyLoggingInterceptorsDisabled.loggingInterceptor())

  val service1 = new TestService(
    requestLogger = messagePayloadsEnabledLogger,
    loggerFactory = loggerFactory,
  )

  val service2 = new TestService(
    requestLogger = messagePayloadsDisabledLogger,
    loggerFactory = loggerFactory,
  )

  private val port1 = UniquePortGenerator.next.unwrap
  private val port2 = UniquePortGenerator.next.unwrap

  private lazy val host = "localhost"

  private lazy val route1: Route =
    PekkoHttpServerInterpreter(route1Options)(executionContext).toRoute(service1.endpoints())
  private lazy val route2: Route =
    PekkoHttpServerInterpreter(route2Options)(executionContext).toRoute(service2.endpoints())

  private var binding1: Http.ServerBinding = _
  private var binding2: Http.ServerBinding = _

  override def beforeAll(): Unit = {
    binding1 = Http()(system).newServerAt(host, port1).bind(route1).futureValue
    binding2 = Http()(system).newServerAt(host, port2).bind(route2).futureValue
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    binding1.unbind().futureValue
    binding2.unbind().futureValue
    super.afterAll()
  }

  "Logger with messagePayloads=true" should {

    val testCases = Table(
      (
        "description",
        "contentType",
        "entity",
        "shouldLogBody",
        "expectedContentType",
        "expectedContentLength",
      ),
      (
        "application/json Strict",
        MediaType.applicationWithOpenCharset("json"),
        "Strict",
        true,
        "application/json",
        s"Some($bodyLength)",
      ),
      (
        "text/plain Strict",
        MediaType.text("plain"),
        "Strict",
        true,
        "text/plain; charset=UTF-8",
        s"Some($bodyLength)",
      ),
      (
        "application/octet-stream Strict",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Strict",
        false,
        "application/octet-stream",
        s"Some($bodyLength)",
      ),
      (
        "application/json Default",
        MediaType.applicationWithOpenCharset("json"),
        "Default",
        true,
        "application/json",
        s"Some($bodyLength)",
      ),
      (
        "text/plain Default",
        MediaType.text("plain"),
        "Default",
        true,
        "text/plain; charset=UTF-8",
        s"Some($bodyLength)",
      ),
      (
        "application/octet-stream Default",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Default",
        false,
        "application/octet-stream",
        s"Some($bodyLength)",
      ),
      (
        "application/json Chunked",
        MediaType.applicationWithOpenCharset("json"),
        "Chunked",
        true,
        "application/json",
        "None",
      ),
      (
        "text/plain Chunked",
        MediaType.text("plain"),
        "Chunked",
        true,
        "text/plain; charset=UTF-8",
        "None",
      ),
      (
        "application/octet-stream Chunked",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Chunked",
        false,
        "application/octet-stream",
        "None",
      ),
      (
        "application/json Chunked-Multi",
        MediaType.applicationWithOpenCharset("json"),
        "Chunked-Multi",
        true,
        "application/json",
        "None",
      ),
      (
        "text/plain Chunked-Multi",
        MediaType.text("plain"),
        "Chunked-Multi",
        true,
        "text/plain; charset=UTF-8",
        "None",
      ),
      (
        "application/octet-stream Chunked-Multi",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Chunked-Multi",
        false,
        "application/octet-stream",
        "None",
      ),
    )

    forAll(testCases) {
      (
          description,
          mediaType,
          entityType,
          shouldLogBody,
          expectedContentType,
          expectedContentLength,
      ) =>
        s"${if (shouldLogBody) "log" else "not log"} request body for $description" in {
          val contentType = ContentType.WithCharset(mediaType, HttpCharsets.`UTF-8`)
          val entity = entityType match {
            case "Strict" => createStrictEntity(contentType)
            case "Default" => createDefaultEntity(contentType)
            case "Chunked" => createSingleChunkedEntity(contentType)
            case "Chunked-Multi" => createMultiChunkedEntity(contentType)
          }
          loggerFactory
            .assertLogsSeq(SuppressionRule.forLogger[ApiRequestLogger])(
              call(port1, entity),
              logSeq => {
                val headerPart =
                  s"received a message \nContentType: Some($expectedContentType)\nContentLength: $expectedContentLength\nParameters: []"
                val bodyPart = "Body: "
                if (shouldLogBody) {
                  logSeq.exists(logEntry =>
                    logEntry.message.contains(headerPart) && logEntry.message.contains(bodyPart)
                      && logEntry.message
                        .substring(logEntry.message.indexOf("\nContentType"))
                        .length <= ApiLoggingConfig.defaultMaxStringLength + "...".length
                  ) shouldBe true
                } else {
                  logSeq.exists(logEntry =>
                    logEntry.message.contains(headerPart) && !logEntry.message
                      .contains(bodyPart)
                  ) shouldBe true
                }
              },
            )
            .map(_ => succeed)
        }
    }
  }

  "Logger with messagePayloads=false" should {

    val testCases = Table(
      (
        "description",
        "contentType",
        "entityType",
      ),
      (
        "application/json Strict",
        MediaType.applicationWithOpenCharset("json"),
        "Strict",
      ),
      (
        "text/plain Strict",
        MediaType.text("plain"),
        "Strict",
      ),
      (
        "application/octet-stream Strict",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Strict",
      ),
      (
        "application/json Default",
        MediaType.applicationWithOpenCharset("json"),
        "Default",
      ),
      (
        "text/plain Default",
        MediaType.text("plain"),
        "Default",
      ),
      (
        "application/octet-stream Default",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Default",
      ),
      (
        "application/json Chunked",
        MediaType.applicationWithOpenCharset("json"),
        "Chunked",
      ),
      (
        "text/plain Chunked",
        MediaType.text("plain"),
        "Chunked",
      ),
      (
        "application/octet-stream Chunked",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Chunked",
      ),
      (
        "application/json Chunked-Multi",
        MediaType.applicationWithOpenCharset("json"),
        "Chunked-Multi",
      ),
      (
        "text/plain Chunked-Multi",
        MediaType.text("plain"),
        "Chunked-Multi",
      ),
      (
        "application/octet-stream Chunked-Multi",
        MediaType.applicationWithOpenCharset("octet-stream"),
        "Chunked-Multi",
      ),
    )

    forAll(testCases) {
      (
          description,
          mediaType,
          entityType,
      ) =>
        s"not log message details for $description" in {
          val contentType = ContentType.WithCharset(mediaType, HttpCharsets.`UTF-8`)
          val entity = entityType match {
            case "Strict" => createStrictEntity(contentType)
            case "Default" => createDefaultEntity(contentType)
            case "Chunked" => createSingleChunkedEntity(contentType)
            case "Chunked-Multi" => createMultiChunkedEntity(contentType)
          }
          loggerFactory
            .assertLogsSeq(SuppressionRule.forLogger[ApiRequestLogger])(
              call(port2, entity),
              logSeq => {
                val headerPart =
                  s"received a message"
                val messagePartRegex =
                  s"\nContentType:.*\nContentLength:.*\nParameters: \\[\\]\nBody: .*"
                logSeq.exists(logEntry =>
                  logEntry.message.contains(headerPart) && !logEntry.message
                    .matches(messagePartRegex)
                ) shouldBe true
              },
            )
            .map(_ => succeed)
        }
    }
  }

  private def call(port: Int, entity: RequestEntity): Future[HttpResponse] = {
    val request = HttpRequest(uri = s"http://localhost:$port/v2/test/endpoint").withEntity(entity)
    Http()(system).singleRequest(request)
  }

  private def createStrictEntity(contentType: ContentType): RequestEntity =
    HttpEntity.Strict(
      contentType = contentType,
      data = ByteString.fromArray("1".repeat(bodyLength).getBytes(StandardCharsets.UTF_8)),
    )

  private def createDefaultEntity(contentType: ContentType): RequestEntity =
    HttpEntity.Default(
      contentType = contentType,
      contentLength = bodyLength.toLong,
      data = Source.single(
        ByteString.fromArray("1".repeat(bodyLength).getBytes(StandardCharsets.UTF_8))
      ),
    )

  private def createSingleChunkedEntity(contentType: ContentType): RequestEntity =
    HttpEntity.Chunked(
      contentType = contentType,
      chunks = Source.single(
        ByteString.fromArray("1".repeat(bodyLength).getBytes(StandardCharsets.UTF_8))
      ),
    )

  private def createMultiChunkedEntity(contentType: ContentType): RequestEntity = {
    val chunksCount = 1000
    val chunkSize = 1024
    val chunkBytes =
      ByteString.fromArray(Random.nextString(chunkSize).getBytes(StandardCharsets.UTF_8))
    HttpEntity.Chunked(
      contentType = contentType,
      chunks = Source.fromIterator(() => Iterator.fill(chunksCount)(chunkBytes)),
    )
  }
}

final case class NoBodyRealData(any: String)

class TestService(
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends Endpoints {
  implicit val config: Configuration = io.circe.generic.extras.Configuration.default.withDefaults
  implicit val codec: Codec[NoBodyRealData] = deriveRelaxedCodec

  implicit val authInterceptor: AuthInterceptor =
    new AuthInterceptor(
      List(AuthServiceWildcard),
      loggerFactory,
      executionContext,
    )

  private lazy val test = v2Endpoint.in(sttp.tapir.stringToPath("test"))

  def endpoints() = List(
    withServerLogic(
      endpoint,
      (_: CallerContext) => (traced: TracedInput[String]) => Future.successful(Right(traced.in)),
    )
  )

  val endpoint: Endpoint[CallerContext, String, (StatusCode, JsCantonError), String, Any] =
    test.get
      .in(sttp.tapir.stringToPath("endpoint"))
      .in(sttp.tapir.stringBody)
      .out(sttp.tapir.stringBody)

}
