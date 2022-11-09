// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.util.ByteString
import akka.http.scaladsl.model.{
  HttpRequest,
  HttpResponse,
  HttpEntity,
  RequestEntity,
  ResponseEntity,
  StatusCodes,
  ContentTypes,
}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.scaladsl.Source
import com.daml.metrics.akkahttp.AkkaUtils._
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.MetricHandle.{Counter, Timer}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.Future
import scala.concurrent.duration._

class AkkaHttpMetricsSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

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
  val testRoute = concat(
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
        Thread.sleep(delayMs)
        s"delayed $delayMs ms"
      }
    },
  )

  private def routeWithGoldenSignalMetrics(route: Route, metrics: TestMetrics): Route = {
    implicit val mc: MetricsContext = MetricsContext.Empty
    AkkaHttpMetrics.goldenSignalsMetrics(
      metrics.httpRequestsTotal,
      metrics.httpErrorsTotal,
      metrics.httpLatency,
      metrics.httpRequestsBytesTotal,
      metrics.httpResponsesBytesTotal,
    ) apply route
  }

  // provides an enviroment to perform the tests
  private def withRouteAndMetrics[T](f: (Route, TestMetrics) => T): T = {
    val metrics = TestMetrics()
    val routeWithMetrics = routeWithGoldenSignalMetrics(Route.seal(testRoute), metrics)
    f(routeWithMetrics, metrics)
  }

  "requests_total" should {
    "collect successful requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get() ~> route
        Get("/simple") ~> route
        Get("/a/bit/deeper") ~> route ~> check {
          metrics.httpRequestsTotalValue should be(3)
        }
      }
    }

    "collect missing routes" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/undefined") ~> route
        Get("/otherUndefined") ~> route ~> check {
          metrics.httpRequestsTotalValue should be(2)
        }
      }
    }

    "collect unauthorized requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/unauthorized") ~> route ~> check {
          metrics.httpRequestsTotalValue should be(1)
        }
      }
    }

    "collect bad requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/badrequest") ~> route ~> check {
          metrics.httpRequestsTotalValue should be(1)
        }
      }
    }

    "collect requests resulting in exceptions" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/exception") ~> route ~> check {
          metrics.httpRequestsTotalValue should be(1)
        }
      }
    }

  }

  "errors_total" should {
    "collect missing routes" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/undefined") ~> route
        Get("/otherUndefined") ~> route ~> check {
          metrics.httpErrorsTotalValue should be(2)
        }
      }
    }

    "collect unauthorized requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/unauthorized") ~> route ~> check {
          metrics.httpErrorsTotalValue should be(1)
        }
      }
    }

    "collect bad requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/badrequest") ~> route ~> check {
          metrics.httpErrorsTotalValue should be(1)
        }
      }
    }

    "collect requests resulting in exceptions" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/exception") ~> route ~> check {
          metrics.httpErrorsTotalValue should be(1)
        }
      }
    }

    "not collect successful requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get() ~> route
        Get("/simple") ~> route
        // needs one failing request, otherwise no value can be found for the metric
        Get("/undefined") ~> route
        Get("/a/bit/deeper") ~> route ~> check {
          metrics.httpErrorsTotalValue should be(1)
        }
      }
    }

  }

  "requests_bytes_total" should {
    "record successful request without payload" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/a/bit/deeper") ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(0L)
        }
      }
    }

    "record string payload size on successful request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`text/plain(UTF-8)`, byteStringText),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteStringTextSize)
        }
      }
    }

    "record byte payload size on successful request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/mirror/200",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
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
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
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
          metrics.httpRequestsBytesTotalValue should be(byteString1Size + byteString2Size)
        }
      }
    }

    "record byte payload size on failed (missing route) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/undefined",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
        }
      }
    }

    "record byte payload size on failed (unauthorized) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/unauthorized",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
        }
      }
    }

    "record byte payload size on failed (bad request) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/badrequest",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
        }
      }
    }

    "record byte payload size on failed (exception) request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(byteString1Size)
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
        ) ~> route
        Get(
          "/exception",
          HttpEntity.Strict(ContentTypes.`application/octet-stream`, byteString1),
        ) ~> route ~> check {
          metrics.httpRequestsBytesTotalValue should be(
            byteStringTextSize + (byteString1Size + byteString2Size) + byteString1Size
          )
        }
      }
    }
  }

  "responses_bytes_total" should {
    "record successful response without payload" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/mirror/200") ~> route ~> check {
          responseAs[String] // force processing the response
          metrics.httpResponsesBytesTotalValue should be(0L)
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
          metrics.httpResponsesBytesTotalValue should be(byteStringTextSize)
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
          metrics.httpResponsesBytesTotalValue should be(byteString1Size)
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
          metrics.httpResponsesBytesTotalValue should be(byteString1Size)
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
          metrics.httpResponsesBytesTotalValue should be(byteString1Size + byteString2Size)
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
          metrics.httpResponsesBytesTotalValue should be(
            response.length.toLong
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
          metrics.httpResponsesBytesTotalValue should be(
            response.length.toLong
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
          metrics.httpResponsesBytesTotalValue should be(
            response.length.toLong
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
          metrics.httpResponsesBytesTotalValue should be(
            response.length.toLong
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
          metrics.httpResponsesBytesTotalValue should be(
            4L + (byteString1Size + byteString2Size) + response.length.toLong
          )
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
          val value = metrics.httpLatencyValue
          value.count should be(1L)
          value.sum should be >= 0L
        }
      }
    }

    "record meaningful duration for a request" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/delay/300") ~> route ~> check {
          val value = metrics.httpLatencyValue
          value.count should be(1L)
          value.sum should be >= 300L
        }
      }
    }

    "record meaningful duration for multiple requests" in {
      withRouteAndMetrics { (route, metrics) =>
        Get("/delay/300") ~> route
        Get("/delay/600") ~> route ~> check {
          val value = metrics.httpLatencyValue
          value.count should be(2L)
          value.sum should be >= 900L
        }
      }
    }
  }

  // creates a response entity from the give request, with copied data
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

object AkkaHttpMetricsSpec {

  // The metrics being tested
  case class TestMetrics(
      httpRequestsTotal: Counter,
      httpErrorsTotal: Counter,
      httpLatency: Timer,
      httpRequestsBytesTotal: Counter,
      httpResponsesBytesTotal: Counter,
  ) {

    import TestMetrics._

    def httpRequestsTotalValue: Long = getCounterValue(httpRequestsTotal)
    def httpErrorsTotalValue: Long = getCounterValue(httpErrorsTotal)
    def httpLatencyValue: HistogramData = getHistogramValues(httpLatency)
    def httpRequestsBytesTotalValue: Long = getCounterValue(httpRequestsBytesTotal)
    def httpResponsesBytesTotalValue: Long = getCounterValue(httpResponsesBytesTotal)

  }

  object TestMetrics extends TestMetricsBase {

    // Creates a new set of metrics, for one test
    def apply(): TestMetrics = {
      val testNumber = testNumbers.getAndIncrement()
      val baseName = MetricName(s"test-$testNumber")

      val httpRequestsTotalName = baseName :+ "requests_total"
      val httpErrorsTotalName = baseName :+ "errors_total"
      val httpLatencyName = baseName :+ "requests_duration_seconds"
      val httpRequestsBytesTotalName = baseName :+ "requests_bytes_total"
      val httpResponsesBytesTotalName = baseName :+ "responses_bytes_total"

      val httpRequestsTotal = metricFactory.counter(httpRequestsTotalName)
      val httpErrorsTotal = metricFactory.counter(httpErrorsTotalName)
      val httpLatency = metricFactory.timer(httpLatencyName)
      val httpRequestsBytesTotal = metricFactory.counter(httpRequestsBytesTotalName)
      val httpResponsesBytesTotal = metricFactory.counter(httpResponsesBytesTotalName)

      TestMetrics(
        httpRequestsTotal,
        httpErrorsTotal,
        httpLatency,
        httpRequestsBytesTotal,
        httpResponsesBytesTotal,
      )
    }
  }

}
