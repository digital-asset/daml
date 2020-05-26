// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.protoc.plugins.akka

import com.google.protobuf.Descriptors.{MethodDescriptor, ServiceDescriptor}
import scalapb.compiler.FunctionalPrinter.PrinterEndo
import scalapb.compiler._

final class AkkaGrpcServicePrinter(service: ServiceDescriptor, params: GeneratorParams)
    extends DescriptorImplicits(params, Seq(service.getFile)) {
  private[this] val killSwitchName = s""""${service.getName}KillSwitch ${System.nanoTime()}""""

  private[this] def observer(typeParam: String): String = s"$streamObserver[$typeParam]"

  private[this] def serviceMethodSignature(method: MethodDescriptor): PrinterEndo = { p =>
    method.streamType match {
      case StreamType.Unary => p
      case StreamType.ClientStreaming => p
      case StreamType.ServerStreaming =>
        p.add(s"def ${method.name}(")
          .indent
          .add(s"request: ${method.inputType.scalaType},")
          .add(s"responseObserver: ${observer(method.outputType.scalaType)}")
          .outdent
          .add(s"): Unit = {")
          .indent
          .add("if (closed.get()) {")
          .indent
          .add("responseObserver.onError(closingError)")
          .outdent
          .add("} else {")
          .indent
          .add(
            "val sink = com.daml.grpc.adapter.server.akka.ServerAdapter.toSink(responseObserver)")
          .add(s"${method.name}Source(request).via(killSwitch.flow).runWith(sink)")
          .add("()")
          .outdent
          .add("}")
          .outdent
          .add("}")
          .add(
            s"protected def ${method.name}Source(request: ${method.inputType.scalaType}): akka.stream.scaladsl.Source[${method.outputType.scalaType}, akka.NotUsed]")
          .newline
      case StreamType.Bidirectional =>
        p
    }
  }

  private[this] def traitBody: PrinterEndo = {
    val endos: PrinterEndo = { p =>
      p.call(service.methods.map(m => serviceMethodSignature(m)): _*)
    }

    p =>
      p.add("protected implicit def esf: com.daml.grpc.adapter.ExecutionSequencerFactory")
        .add("protected implicit def mat: akka.stream.Materializer")
        .call(closureUtils)
        .newline
        .call(endos)
  }

  private def closureUtils: PrinterEndo = { p =>
    p.newline
      .add(s"protected val killSwitch = akka.stream.KillSwitches.shared($killSwitchName)")
      .add("protected val closed = new java.util.concurrent.atomic.AtomicBoolean(false)")
      .add("protected def closingError = new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription(\"Server is shutting down\")) with scala.util.control.NoStackTrace")
      .add("def close(): Unit = {")
      .indent
      .add("if (closed.compareAndSet(false, true)) killSwitch.abort(closingError)")
      .outdent
      .add("}")
  }
  private[this] val streamObserver = "_root_.io.grpc.stub.StreamObserver"

  def printService(printer: FunctionalPrinter): Option[FunctionalPrinter] = {

    val hasStreamingEndpoint: Boolean = service.methods.exists(_.isServerStreaming)

    if (hasStreamingEndpoint) Some {
      printer
        .add(
          "package " + service.getFile.scalaPackageName,
          "",
          s"trait ${service.name}AkkaGrpc extends ${service.getName}Grpc.${service.getName} with AutoCloseable {"
        )
        .indent
        .call(traitBody)
        .newline
        .outdent
        .add("}")
    } else None
  }
}
