// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.protoc.plugins.akka

import com.google.protobuf.Descriptors.{MethodDescriptor, ServiceDescriptor}
import scalapb.compiler.FunctionalPrinter.PrinterEndo
import scalapb.compiler.{DescriptorImplicits, FunctionalPrinter, StreamType}

final class AkkaGrpcServicePrinter(
    service: ServiceDescriptor
)(implicit descriptorImplicits: DescriptorImplicits) {
  import descriptorImplicits._

  private val StreamObserver = "_root_.io.grpc.stub.StreamObserver"

  def printService(printer: FunctionalPrinter): Option[FunctionalPrinter] = {
    val hasStreamingEndpoint: Boolean = service.methods.exists(_.isServerStreaming)

    if (hasStreamingEndpoint) Some {
      printer
        .add(
          "package " + service.getFile.scalaPackage.fullName,
          "",
          s"trait ${service.name}AkkaGrpc extends ${service.getName}Grpc.${service.getName} with AutoCloseable {",
        )
        .call(traitBody)
        .add("}")
    }
    else None
  }

  private def responseType(method: MethodDescriptor): String = method.outputType.scalaType

  private def observer(typeParam: String): String = s"$StreamObserver[$typeParam]"

  private def serviceMethodSignature(method: MethodDescriptor): PrinterEndo = { p =>
    method.streamType match {
      case StreamType.Unary => p
      case StreamType.ClientStreaming => p
      case StreamType.ServerStreaming =>
        p
          .add(s"def ${method.name}(")
          .indent
          .add(s"request: ${method.inputType.scalaType},")
          .add(s"responseObserver: ${observer(responseType(method))}")
          .outdent
          .add("): Unit =")
          .indent
          .add("synchronized {")
          .indent
          .add("if (closed) {")
          .indent
          .add(
            "responseObserver.onError(com.daml.grpc.adapter.server.akka.ServerAdapter.closingError())"
          )
          .outdent
          .add("} else {")
          .indent
          .add(
            s"val killSwitch = com.daml.grpc.adapter.server.akka.ServerAdapter.registerStream(${method.name}Source(request), responseObserver)"
          )
          .add("killSwitches = killSwitch :: killSwitches")
          .outdent
          .add("}")
          .outdent
          .add("}")
          .outdent
          .newline
          .add(s"protected def ${method.name}Source(")
          .indent
          .add(s"request: ${method.inputType.scalaType}")
          .outdent
          .add(s"): akka.stream.scaladsl.Source[${responseType(method)}, akka.NotUsed]")
          .newline
      case StreamType.Bidirectional => p
    }
  }

  private def traitBody: PrinterEndo = {
    val endos: PrinterEndo = { p =>
      p.newline
        .call(service.methods.map(m => serviceMethodSignature(m)): _*)
    }

    p =>
      p.indent
        .add("protected implicit def esf: com.daml.grpc.adapter.ExecutionSequencerFactory")
        .add("protected implicit def mat: akka.stream.Materializer")
        .call(closeMethod)
        .call(endos)
        .outdent
  }

  private def closeMethod: PrinterEndo = { p =>
    p
      .add("@volatile private var closed = false")
      .add("@volatile private var killSwitches = List.empty[akka.stream.KillSwitch]")
      .newline
      .add("def close(): Unit =")
      .indent
      .add("synchronized {")
      .indent
      .add("if (!closed) {")
      .indent
      .add("closed = true")
      .add(
        "killSwitches.foreach(_.abort(com.daml.grpc.adapter.server.akka.ServerAdapter.closingError()))"
      )
      .outdent
      .add("}")
      .outdent
      .add("}")
      .outdent
  }
}
