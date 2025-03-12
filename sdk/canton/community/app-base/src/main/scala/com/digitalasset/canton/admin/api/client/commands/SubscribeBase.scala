// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  ServerEnforcedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.ForwardingStreamObserver
import io.grpc.Context
import io.grpc.stub.StreamObserver

import scala.concurrent.Future

trait SubscribeBase[Req, Resp, Res] extends GrpcAdminCommand[Req, AutoCloseable, AutoCloseable] {
  // The subscription should never be cut short because of a gRPC timeout
  override def timeoutType: TimeoutType = ServerEnforcedTimeout

  def observer: StreamObserver[Res]

  def doRequest(
      service: this.Svc,
      request: Req,
      rawObserver: StreamObserver[Resp],
  ): Unit

  def extractResults(response: Resp): IterableOnce[Res]

  implicit def loggingContext: ErrorLoggingContext

  override protected def submitRequest(
      service: this.Svc,
      request: Req,
  ): Future[AutoCloseable] = {
    val rawObserver = new ForwardingStreamObserver[Resp, Res](observer, extractResults)
    val context = Context.current().withCancellation()
    context.run(() => doRequest(service, request, rawObserver))
    Future.successful(context)
  }

  override protected def handleResponse(response: AutoCloseable): Either[String, AutoCloseable] =
    Right(
      response
    )
}
