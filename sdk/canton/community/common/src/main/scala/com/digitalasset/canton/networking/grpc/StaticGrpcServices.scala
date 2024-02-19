// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc

import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import io.grpc.*

import scala.jdk.CollectionConverters.*

/** Stubbed GRPC services to provide simple responses rather than a typical full implementation. */
object StaticGrpcServices {

  val notSupportedByCommunityStatus = Status.UNIMPLEMENTED
    .withDescription(
      "This method is unsupported by the Community edition of canton. Please see https://www.digitalasset.com/products/daml-enterprise for details on obtaining Canton Enterprise."
    )

  /** Return a `UNIMPLEMENTED` error for any methods called on this service mentioning that the service is only
    * supported in canton Enterprise. Also log a warning so the node operator is aware this method was called.
    */
  def notSupportedByCommunity(
      descriptor: ServiceDescriptor,
      logger: TracedLogger,
  ): ServerServiceDefinition =
    forService(descriptor) { method =>
      implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
      // the service name typically includes the full package details, so ideally do away with this if it parses
      val shortServiceName =
        method.getServiceName.split('.').lastOption.getOrElse(method.getServiceName)
      logger.warn(
        s"This Community edition of canton does not support the operation: $shortServiceName.${method.getBareMethodName}. Please see https://www.digitalasset.com/products/daml-enterprise for details on obtaining Canton Enterprise edition."
      )

      notSupportedByCommunityStatus
    }

  def notSupportedByCommunity(
      descriptor: ServiceDescriptor,
      loggerFactory: NamedLoggerFactory,
  ): ServerServiceDefinition = {
    val logger = TracedLogger(loggerFactory.getLogger(StaticGrpcServices.getClass))
    notSupportedByCommunity(descriptor, logger)
  }

  /** Stub all methods on the provided service descriptor using the given handler. */
  def forService(
      descriptor: ServiceDescriptor
  )(handler: MethodDescriptor[_, _] => Status): ServerServiceDefinition = {
    val builder = ServerServiceDefinition.builder(descriptor)

    descriptor.getMethods.asScala.foreach { method =>
      builder
        .addMethod(ServerMethodDefinition.create(method, mkClosingCallHandler(method, handler)))
        .discard[ServerServiceDefinition.Builder]
    }

    builder.build()
  }

  /** Creates a call handler that immediately closes the server call with the generated status. */
  private def mkClosingCallHandler[Req, Res](
      method: MethodDescriptor[_, _],
      handler: MethodDescriptor[_, _] => Status,
  ): ServerCallHandler[Req, Res] = { (call: ServerCall[Req, Res], _: Metadata) =>
    new ServerCall.Listener[Req] {
      call.close(
        handler(method),
        new Metadata(),
      )
    }
  }
}
