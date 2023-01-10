// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.util.ByteString
import akka.http.scaladsl.model.{
  ContentTypes,
  HttpEntity,
  HttpRequest,
  HttpResponse,
  RequestEntity,
  ResponseEntity,
  StatusCodes,
}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Source
import com.daml.metrics.akkahttp.AkkaUtils._
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.MetricHandle.{Histogram, Meter, Timer}
import com.daml.metrics.api.testing.{InMemoryMetricsFactory, MetricValues}
import com.daml.metrics.http.HttpMetrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpMetricsInterseptorSpec
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with MetricValues {

  import AkkaHttpMetricsSpec._

  // test data
  private val byteString1 = ByteString(Array[Byte](1, 12, -7, -124, 0, 127))
  private val byteString1Size = byteString1.length

  private val byteString2 = ByteString(Array[Byte](-4, -3, -2, -1, 0, 1, 2, 3, 4))
  private val byteString2Size = byteString2.length

  private val byteStringTextData = "hello"
  private val byteStringText = ByteString(byteStringTextData)
  private val byteStringTextSize = byteStringTextData.getBytes.length.toLong

  // The route used for testing
  // extractStrictEntity is used to force reading the request entity
  private val testRoute = concat(
    pathSingleSlash {
      Directives.extractStrictEntity(2.seconds) { _ =>
        Directives.complete("root")
      }
    },
    path("simple") {
      Directives.complete("simple")
    },
    path("a" / "bit" / "deeper") {
      Directives.extractStrictEntity(2.seconds) { _ =>
        Directives.complete("a bit deeper")
      }
    },
    path("unauthorized") {
      Directives.extractStrictEntity(2.seconds) { _ =>
        Directives.complete(StatusCodes.Unauthorized)
      }
    },
    path("badrequest") {
      Directives.extractStrictEntity(2.seconds) { _ =>
        Directives.complete(StatusCodes.BadRequest)
      }
    },
    path("exception") {
      Directives.complete(throw new NotImplementedError)
    },
    path("mirror" / IntNumber) { statusCode: Int =>
      {
        handle { request: HttpRequest =>
          {
            mirrorRequestEntity(request.entity).map { entity =>
              HttpResponse(status = statusCode, entity = entity)
            }
          }
        }
      }
    },
    path("delay" / LongNumber) { delayMs =>
      Directives.complete {
        Thread.sleep(delayMs + 100)
        s"delayed $delayMs ms"
      }
    },
  )

  private def routeWithRateDurationSizeMetrics(route: Route, metrics: TestMetrics): Route = {
    HttpMetricsInterceptor.rateDurationSizeMetrics(
      metrics
    ) apply route
  }

  // Provides an environment to perform the tests.
  private def withRouteAndMetrics[T](f: (Route, TestMetrics) => T): T = {
    val metrics = TestMetrics()
    val routeWithMetrics = routeWithRateDurationSizeMetrics(Route.seal(testRoute), metrics)
    f(routeWithMetrics, metrics)
  }

  "requests_total" should {
    "collect successful requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get() ~> route
        Get("/simple") ~> route
        Get("/a/bit/deeper") ~> route ~> check {
          metrics.requestsTotalValue("GET", "/") should be(1L)
          metrics.requestsTotalValue("GET", "/simple") should be(1L)
          metrics.requestsTotalValue("GET", "/a/bit/deeper") should be(1L)
        }
      }
    }

    "collect missing routes" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/undefined") ~> route
        Get("/otherUndefined") ~> route ~> check {
          metrics.requestsTotalValue("GET", "/undefined") should be(1L)
          metrics.requestsTotalValue("GET", "/otherUndefined") should be(1L)
        }
      }
    }

    "collect unauthorized requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/unauthorized") ~> route ~> check {
          metrics.requestsTotalValue should be(1L)
        }
      }
    }

    "collect bad requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/badrequest") ~> route ~> check {
          metrics.requestsTotalValue should be(1L)
        }
      }
    }

    "collect requests resulting in exceptions" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/exception") ~> route ~> check {
          metrics.requestsTotalValue should be(1L)
        }
      }
    }

    "contains all the labels" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/simple") ~> route
        Post("/badrequest") ~> route ~> check {
          metrics.requestsTotal.valueFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_status", "200"),
            LabelFilter("http_verb", "GET"),
            LabelFilter("path", "/simple"),
          ) should be(1L)
          metrics.requestsTotal.valueFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_status", "400"),
            LabelFilter("http_verb", "POST"),
            LabelFilter("path", "/badrequest"),
          ) should be(1L)
        }
      }
    }

  }

  "requests_bytes" should {
    "record successful request without payload" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/a/bit/deeper") ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(0L))
        }
      }
    }

    "record string payload size on successful request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteStringTextSize))
        }
      }
    }

    "record byte payload size on successful request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on successful request, actual size, not content length" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Default(
            ContentTypes.`application/octet-stream`,
            4,
            Source.single(byteString1),
          ),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on successful requests, chunked" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Chunked(
            ContentTypes.`application/octet-stream`,
            Source(List(byteString1, byteString2)),
          ),
        ) ~> route ~> check {
          responseAs[String] // force processing the request
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size + byteString2Size))
        }
      }
    }

    "record byte payload size on failed (missing route) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/undefined",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on failed (unauthorized) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/unauthorized",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on failed (bad request) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/badrequest",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on failed (exception) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on multiple request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/", HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText)) ~> route
        Get(
          "/mirror/200",
          HttpEntity.Chunked(
            ContentTypes.`application/octet-stream`,
            Source(List(byteString1, byteString2)),
          ),
        ) ~> route ~> check {
          responseAs[String] // force processing the request
        }
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.requestsPayloadBytesValues("GET", "/") should be(Seq(byteStringTextSize))
          metrics.requestsPayloadBytesValues("GET", "/mirror/200") should be(
            Seq(byteString1Size + byteString2Size)
          )
          metrics.requestsPayloadBytesValues("GET", "/exception") should be(Seq(byteString1Size))
        }
      }
    }

    "contains all the labels" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/simple",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route
        Post(
          "/badrequest",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString2),
        ) ~> route ~> check {
          metrics.requestsPayloadBytes.valuesFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_verb", "GET"),
            LabelFilter("path", "/simple"),
          ) should be(Seq(byteString1Size))
          metrics.requestsPayloadBytes.valuesFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_verb", "POST"),
            LabelFilter("path", "/badrequest"),
          ) should be(Seq(byteString2Size))
        }
      }
    }
  }

  "responses_bytes" should {
    "record successful response without payload" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/mirror/200") ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.responsesPayloadBytesValues should be(Seq(0L))
        }
      }
    }

    "record string payload size on successful response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.responsesPayloadBytesValues should be(Seq(byteStringTextSize))
        }
      }
    }

    "record byte payload size on successful response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.responsesPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on successful response, actual size, not content length" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Default(
            ContentTypes.`application/octet-stream`,
            4,
            Source.single(byteString1),
          ),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.responsesPayloadBytesValues should be(Seq(byteString1Size))
        }
      }
    }

    "record byte payload size on successful response, chunked" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Chunked(
            ContentTypes.`application/octet-stream`,
            Source(List(byteString1, byteString2)),
          ),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.responsesPayloadBytesValues should be(Seq(byteString1Size + byteString2Size))
        }
      }
    }

    "record byte payload size on failed (missing route) response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/undefined",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          val response = responseAs[String]
          metrics.responsesPayloadBytesValues should be(
            Seq(response.length.toLong)
          )
        }
      }
    }

    "record byte payload size on failed (unauthorized) response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/unauthorized",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          val response = responseAs[String]
          metrics.responsesPayloadBytesValues should be(
            Seq(response.length.toLong)
          )
        }
      }
    }

    "record byte payload size on failed (bad request) response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/badrequest",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          val response = responseAs[String]
          metrics.responsesPayloadBytesValues should be(
            Seq(response.length.toLong)
          )
        }
      }
    }

    "record byte payload size on failed (exception) response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          val response = responseAs[String]
          metrics.responsesPayloadBytesValues should be(
            Seq(response.length.toLong)
          )
        }
      }
    }

    "record byte payload size on multiple response" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/",
          HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
        }
        Get(
          "/mirror/200",
          HttpEntity.Chunked(
            ContentTypes.`application/octet-stream`,
            Source(List(byteString1, byteString2)),
          ),
        ) ~> route ~> check {
          responseAs[String] // force processing the response
        }
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          val response = responseAs[String]
          metrics.responsesPayloadBytesValues("GET", "/") should be(Seq(4L))
          metrics.responsesPayloadBytesValues("GET", "/mirror/200") should be(
            Seq(byteString1Size + byteString2Size)
          )
          metrics.responsesPayloadBytesValues("GET", "/exception") should be(
            Seq(response.length.toLong)
          )
        }
      }
    }

    "contains all the labels" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route
        Post(
          "/badrequest",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString2),
        ) ~> route ~> check {
          val response = responseAs[String]

          metrics.responsesPayloadBytes.valuesFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_status", "200"),
            LabelFilter("http_verb", "GET"),
            LabelFilter("path", "/mirror/200"),
          ) should be(Seq(byteString1Size))
          metrics.responsesPayloadBytes.valuesFilteredOnLabels(
            LabelFilter("host", "example.com"),
            LabelFilter("http_status", "400"),
            LabelFilter("http_verb", "POST"),
            LabelFilter("path", "/badrequest"),
          ) should be(Seq(response.length.toLong))
        }
      }
    }
  }

  "request_bytes_total and responses_bytes_total support" should {
    "not alter string data" in {
      withRouteAndMetrics { (route, _) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText),
        ) ~> route ~> check {
          responseAs[String] should be(byteStringTextData)
        }
      }
    }

    "not alter binary data" in {
      withRouteAndMetrics { (route, _) =>
        Get(
          "/mirror/200",
          HttpEntity.Default(
            ContentTypes.`application/octet-stream`,
            4,
            Source.single(byteString1),
          ),
        ) ~> route ~> check {
          responseAs[Array[Byte]] should equal(byteString1)
        }
      }
    }

    "record byte payload size on successful response, chunked" in {
      withRouteAndMetrics { (route, _) =>
        Get(
          "/mirror/200",
          HttpEntity.Chunked(
            ContentTypes.`application/octet-stream`,
            Source(List(byteString1, byteString2)),
          ),
        ) ~> route ~> check {
          responseAs[Array[Byte]] should equal(byteString1 ++ byteString2)
        }
      }
    }

  }

  "request_duration_seconds" should {
    "record duration of any request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/") ~> route ~> check {
          val values = metrics.latencyValues
          values.size should be(1L)
          values(0) should be >= 0L
        }
      }
    }

    "record meaningful duration for a request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/delay/300") ~> route ~> check {
          val values = metrics.latencyValues
          values.size should be(1L)
          values(0) should be >= 300L
        }
      }
    }

    "record meaningful duration for multiple requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/delay/300") ~> route
        Get("/delay/300") ~> route
        Get("/delay/500") ~> route ~> check {
          val values300 = metrics.latencyValues("GET", "/delay/300")
          values300.size should be(2L)
          values300(0) should be >= 300L
          values300(1) should be >= 300L
          val values500 = metrics.latencyValues("GET", "/delay/500")
          values500.size should be(1L)
          values500(0) should be >= 500L
        }
      }
    }

    "contains all the labels" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/delay/300") ~> route ~> check {
          metrics.latency
            .valuesFilteredOnLabels(
              LabelFilter("host", "example.com"),
              LabelFilter("http_verb", "GET"),
              LabelFilter("path", "/delay/300"),
            )
            .size should be(1L)
        }
      }
    }
  }

  // Creates a response entity from the given request, with copied data.
  private def mirrorRequestEntity(request: RequestEntity): Future[ResponseEntity] =
    request match {
      case HttpEntity.Default(contentType, contentLength, data) =>
        duplicateSource(data, duplicate).map(
          HttpEntity.Default(contentType, contentLength, _)
        )
      case HttpEntity.Strict(contentType, data) =>
        Future(HttpEntity.Strict(contentType, duplicate(data)))
      case HttpEntity.Chunked(contentType, chunks) =>
        duplicateSource[HttpEntity.ChunkStreamPart](chunks, duplicate).map(
          HttpEntity.Chunked(contentType, _)
        )
    }

}

object AkkaHttpMetricsSpec extends MetricValues {

  private val metricsFactory = InMemoryMetricsFactory
  // The metrics being tested
  case class TestMetrics(
      requestsTotal: Meter,
      latency: Timer,
      requestsPayloadBytes: Histogram,
      responsesPayloadBytes: Histogram,
  ) extends HttpMetrics {

    def requestsTotalValue: Long = requestsTotal.value
    def requestsTotalValue(method: String, path: String): Long =
      requestsTotal.valueFilteredOnLabels(labelFilters(method, path): _*)
    def latencyValues: Seq[Long] = latency.values
    def latencyValues(method: String, path: String): Seq[Long] =
      latency.valuesFilteredOnLabels(labelFilters(method, path): _*)
    def requestsPayloadBytesValues: Seq[Long] = requestsPayloadBytes.values
    def requestsPayloadBytesValues(method: String, path: String): Seq[Long] =
      requestsPayloadBytes.valuesFilteredOnLabels(labelFilters(method, path): _*)
    def responsesPayloadBytesValues: Seq[Long] = responsesPayloadBytes.values
    def responsesPayloadBytesValues(method: String, path: String): Seq[Long] =
      responsesPayloadBytes.valuesFilteredOnLabels(labelFilters(method, path): _*)

    private def labelFilters(method: String, path: String): Seq[LabelFilter] =
      Seq(LabelFilter("http_verb", method), LabelFilter("path", path))
  }

  object TestMetrics {

    // Creates a new set of metrics, for one test
    def apply(): TestMetrics = {
      val baseName = MetricName("test")
      val requestsTotal = metricsFactory.meter(baseName :+ "requests")
      val latency = metricsFactory.timer(baseName :+ "duration")
      val requestsPayloadBytes = metricsFactory.histogram(baseName :+ "requests" + "payload")
      val responsesPayloadBytes = metricsFactory.histogram(baseName :+ "responses" + "payload")

      TestMetrics(
        requestsTotal,
        latency,
        requestsPayloadBytes,
        responsesPayloadBytes,
      )
    }
  }
}
