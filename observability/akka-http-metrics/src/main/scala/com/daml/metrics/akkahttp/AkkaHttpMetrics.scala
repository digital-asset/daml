// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.stream.scaladsl.{Source, Flow, Sink}
import akka.util.ByteString
import akka.http.scaladsl.model.{RequestEntity, ResponseEntity, HttpEntity, HttpResponse}
import akka.http.scaladsl.server.{Directive, Route, RouteResult}
import akka.http.scaladsl.server.RouteResult._
import com.daml.metrics.api.MetricHandle.{Meter, Timer}
import com.daml.metrics.api.MetricsContext
import scala.concurrent.ExecutionContext
import scala.util.Success
import scala.util.Try

// Support to capture metrics on akka http
object AkkaHttpMetrics {

  // Provides an akka http directive which captures via the passed metrics the following signals:
  //  - total number of requests
  //  - total number of requests resulting in errors
  //  - latency of the requests
  //  - size of the request payloads
  //  - size of the response payloads
  def rateDurationSizeMetrics(
      requestsTotal: Meter,
      errorsTotal: Meter,
      latency: Timer,
      requestsPayloadBytesTotal: Meter,
      responsesPayloadBytesTotal: Meter,
  )(implicit ec: ExecutionContext) =
    Directive { (fn: Unit => Route) => ctx =>
      implicit val metricsContext: MetricsContext = LabelExtractor.labelsFromRequest(ctx.request)

      // process the query, using a copy of the httpRequest, with size metric computation
      val newCtx = ctx.withRequest(
        ctx.request.withEntity(
          requestEntityContentLengthReportMetric(ctx.request.entity, requestsPayloadBytesTotal)
        )
      )
      val result = latency.timeFuture(fn(())(newCtx))

      result.transform { result =>
        result match {
          case Success(Complete(httpResponse)) =>
            metricsWithHttpResponse(
              requestsTotal,
              errorsTotal,
              responsesPayloadBytesTotal,
              httpResponse,
              metricsContext,
            )
          case _ =>
            // record request and failure
            requestsTotal.mark()
            errorsTotal.mark()
            result
        }
      }

    }

  // Split method needed to be able to define a new implicit MetricsContext value.
  private def metricsWithHttpResponse(
      requestsTotal: Meter,
      errorsTotal: Meter,
      responsesPayloadBytesTotal: Meter,
      httpResponse: HttpResponse,
      requestMetricsContext: MetricsContext,
  ): Try[RouteResult] = {
    implicit val metricsContext: MetricsContext =
      LabelExtractor.addLabelsFromResponse(requestMetricsContext, httpResponse)

    // record request
    requestsTotal.mark()
    if (httpResponse.status.isFailure)
      // record failure
      errorsTotal.mark()
    // return a copy of the httpResponse, with size metric computation
    Success(
      Complete(
        httpResponse.withEntity(
          responseEntityContentLengthReportMetric(httpResponse.entity, responsesPayloadBytesTotal)
        )
      )
    )
  }

  // Support for computation and reporting of the size of a requestEntity.
  // For non-strict content, creates a copy of the requestEntity, with embedded support.
  private def requestEntityContentLengthReportMetric(
      requestEntity: RequestEntity,
      metric: Meter,
  )(implicit mc: MetricsContext): RequestEntity =
    requestEntity match {
      case e: HttpEntity.Default =>
        e.copy(data = byteStringSourceLengthReportMetric(e.data, metric))
      case e: HttpEntity.Strict =>
        metric.mark(e.data.length.toLong)
        e
      case e: HttpEntity.Chunked =>
        e.copy(chunks = chunkStreamPartSourceLengthReportMetric(e.chunks, metric))
    }

  // Support for computation and reporting of the size of a responseEntity.
  // For non-strict content, creates a copy of the responseEntity, with embedded support.
  private def responseEntityContentLengthReportMetric(
      responseEntity: ResponseEntity,
      metric: Meter,
  )(implicit mc: MetricsContext): ResponseEntity =
    responseEntity match {
      case e: HttpEntity.Default =>
        e.copy(data = byteStringSourceLengthReportMetric(e.data, metric))
      case e: HttpEntity.Strict =>
        metric.mark(e.data.length.toLong)
        e
      case e: HttpEntity.Chunked =>
        e.copy(chunks = chunkStreamPartSourceLengthReportMetric(e.chunks, metric))
      case e: HttpEntity.CloseDelimited =>
        e.copy(data = byteStringSourceLengthReportMetric(e.data, metric))
    }

  // adds a side flow to the source, to compute and report the total size of the ByteString elements
  private def byteStringSourceLengthReportMetric[Mat](
      source: Source[ByteString, Mat],
      metric: Meter,
  )(implicit mc: MetricsContext): Source[ByteString, Mat] =
    source.alsoTo(
      Flow[ByteString].fold(0L)((acc, d) => acc + d.length).to(Sink.foreach(metric.mark(_)))
    )

  // Adds a side flow to the source, to compute and report the total size of the ChunkStreamPart elements.
  private def chunkStreamPartSourceLengthReportMetric[Mat](
      source: Source[HttpEntity.ChunkStreamPart, Mat],
      metric: Meter,
  )(implicit mc: MetricsContext): Source[HttpEntity.ChunkStreamPart, Mat] =
    source.alsoTo(
      Flow[HttpEntity.ChunkStreamPart]
        .fold(0L)((acc, c) => acc + c.data.length)
        .to(Sink.foreach(metric.mark(_)))
    )

}
