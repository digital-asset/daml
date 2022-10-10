// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import scala.concurrent.ExecutionContext

import akka.util.ByteString
import akka.stream.scaladsl.{Source, Flow, Sink}
import akka.http.scaladsl.model.{RequestEntity, ResponseEntity, HttpEntity}
import akka.http.scaladsl.server.{Directive, Route}
import akka.http.scaladsl.server.RouteResult._

import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle.{Counter, Timer, Histogram}

/** Support to capture metrics on akka http
  */
object AkkaHttpMetrics {

  /** Provides an akka http directive which capture in the given metrics, the following signals:
    *  - total number of requests
    *  - total number of requests resulting in errors
    *  - latency of the requests
    *  - size of the request payloads
    *  - size of the response payloads
    */
  def goldenSignalsMetrics(
      requestsTotal: Counter,
      errorsTotal: Counter,
      latency: Timer,
      requestSize: Histogram,
      responseSize: Histogram,
  )(implicit ec: ExecutionContext) =
    Directive { (fn: Unit => Route) => ctx =>
      // process the query, using a copy of the httpRequest, with size metric computation
      val newCtx = ctx.withRequest(
        ctx.request.withEntity(
          requestEntityContentLenghtReportMetric(ctx.request.entity, requestSize)
        )
      )
      val result = Timed.future(latency, fn(())(newCtx))

      result.transform { result =>
        result match {
          case scala.util.Success(Complete(httpResponse)) =>
            // record request
            requestsTotal.inc()
            if (httpResponse.status.isFailure)
              // record failure
              errorsTotal.inc()
            // return a copy of the httpResponse, with size metric computation
            scala.util.Success(
              Complete(
                httpResponse.withEntity(
                  responseEntityContentLenghtReportMetric(httpResponse.entity, responseSize)
                )
              )
            )
          case _ =>
            // record request and failure
            requestsTotal.inc()
            errorsTotal.inc()
            result
        }
      }

    }

  // support for computation and report of the size of a requestEntity
  // for streaming content, creates a copy of the requestEntity, with embedded support
  private def requestEntityContentLenghtReportMetric(
      requestEntity: RequestEntity,
      metric: Histogram,
  ): RequestEntity =
    requestEntity match {
      case e: HttpEntity.Default =>
        e.copy(data = byteStringSourceLenghtReportMetric(e.data, metric))
      case e: HttpEntity.Strict =>
        metric.update(e.data.length)
        e
      case e: HttpEntity.Chunked =>
        e.copy(chunks = chunkStreamPartSourceLengthReportMetric(e.chunks, metric))

    }

  // support for computation and report of the size of a responseEntity
  // for streaming content, creates a copy of the responseEntity, with embedded support
  private def responseEntityContentLenghtReportMetric(
      responseEntity: ResponseEntity,
      metric: Histogram,
  ): ResponseEntity =
    responseEntity match {
      case e: HttpEntity.Default =>
        e.copy(data = byteStringSourceLenghtReportMetric(e.data, metric))
      case e: HttpEntity.Strict =>
        metric.update(e.data.length)
        e
      case e: HttpEntity.Chunked =>
        e.copy(chunks = chunkStreamPartSourceLengthReportMetric(e.chunks, metric))
      case e: HttpEntity.CloseDelimited =>
        e.copy(data = byteStringSourceLenghtReportMetric(e.data, metric))
    }

  // adds a side flow to the source, to compute and report the total size of the ByteString elements
  private def byteStringSourceLenghtReportMetric[Mat](
      source: Source[ByteString, Mat],
      metric: Histogram,
  ): Source[ByteString, Mat] =
    source.alsoTo(
      Flow[ByteString].fold(0)((acc, d) => acc + d.length).to(Sink.foreach(metric.update(_)))
    )

  // adds a side flow to the source, to compute and report the total size of the ChunckStreamPart elements
  private def chunkStreamPartSourceLengthReportMetric[Mat](
      source: Source[HttpEntity.ChunkStreamPart, Mat],
      metric: Histogram,
  ): Source[HttpEntity.ChunkStreamPart, Mat] =
    source.alsoTo(
      Flow[HttpEntity.ChunkStreamPart]
        .fold(0)((acc, c) => acc + c.data.length)
        .to(Sink.foreach(metric.update(_)))
    )

}
