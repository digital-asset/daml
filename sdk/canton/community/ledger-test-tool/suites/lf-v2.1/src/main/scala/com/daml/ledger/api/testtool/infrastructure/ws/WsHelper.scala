// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.ws

import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import sttp.capabilities.WebSockets
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.client.sttp.WebSocketToPipe
import sttp.tapir.model.WebSocketFrameDecodeFailure
import sttp.tapir.{DecodeResult, WebSocketBodyOutput}
import sttp.ws.{WebSocket, WebSocketFrame}

import scala.concurrent.{ExecutionContext, Future}

/* This is a copy of sttp.tapir.client.sttp.ws.pekkohttp.WebSocketToPekkoPipe
  with a change that org.apache.pekko.stream.StreamDetachedException is ignored.
  It allows to continue processing of client side messages, despite stream being closed.
 */
class WebSocketToPekkoPipe[R](implicit ec: ExecutionContext) extends WebSocketToPipe[R] {
  override type S = PekkoStreams
  override type F[X] = Future[X]

  override def apply[REQ, RESP](
      s: Any
  )(ws: WebSocket[Future], o: WebSocketBodyOutput[Any, REQ, RESP, ?, PekkoStreams]): Any = {

    val sink = Flow[REQ]
      .map(o.requests.encode)
      .mapAsync(1)(ws.send(_, isContinuation = false))
      .to(Sink.ignore)

    val source = Source
      .repeat(() => ws.receive())
      .mapAsync(1)(lazyFuture => lazyFuture())
      .mapAsync(1) {
        case _: WebSocketFrame.Close if !o.decodeCloseResponses =>
          Future.successful(Right(None): Either[Unit, Option[RESP]])
        case _: WebSocketFrame.Pong if o.ignorePong =>
          Future.successful(Left(()): Either[Unit, Option[RESP]])
        case WebSocketFrame.Ping(p) if o.autoPongOnPing =>
          ws.send(WebSocketFrame.Pong(p)).map(_ => Left(()): Either[Unit, Option[RESP]])
        case f =>
          o.responses.decode(f) match {
            case failure: DecodeResult.Failure =>
              Future.failed(new WebSocketFrameDecodeFailure(f, failure))
            case DecodeResult.Value(v) =>
              Future.successful(Right(Some(v)): Either[Unit, Option[RESP]])
          }
      }
      .collect { case Right(d) => d }
      .takeWhile(_.isDefined)
      .recover { error =>
        error match {
          case _: org.apache.pekko.stream.StreamDetachedException => None
          case other => throw other
        }
      }
      .collect { case Some(d) => d }

    Flow.fromSinkAndSource(sink, source): Flow[REQ, RESP, Any]
  }
}

object WsHelper {
  implicit def webSocketsSupportedForPekkoStreams(implicit
      ec: ExecutionContext
  ): WebSocketToPipe[PekkoStreams with WebSockets] =
    new WebSocketToPekkoPipe[PekkoStreams with WebSockets]
}
