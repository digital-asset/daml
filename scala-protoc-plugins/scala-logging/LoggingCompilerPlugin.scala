// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import com.google.protobuf.Descriptors.{FileDescriptor, MethodDescriptor, ServiceDescriptor}
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import protocbridge.{Artifact, ProtocCodeGenerator}
import scala.reflect.io.Streamable
import scalapb.compiler.FunctionalPrinter.PrinterEndo
import scalapb.compiler.ProtobufGenerator.parseParameters
import scalapb.compiler.{
  DescriptorImplicits,
  FunctionalPrinter,
  GeneratorException,
  ProtoValidation,
  StreamType
}
import scalapb.options.compiler.Scalapb

import scala.collection.JavaConverters._

object LoggingCompilerPlugin {
  def main(args: Array[String]): Unit = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(Streamable.bytes(System.in), registry)
    System.out.write(LoggingGenerator.handleCodeGeneratorRequest(request).toByteArray)
  }
}

object LoggingGenerator extends ProtocCodeGenerator {
  override def run(req: Array[Byte]): Array[Byte] = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(req, registry)
    handleCodeGeneratorRequest(request).toByteArray
  }

  override def suggestedDependencies: Seq[Artifact] = Seq(
    Artifact(
      "com.thesamet.scalapb",
      "scalapb-runtime",
      scalapb.compiler.Version.scalapbVersion,
      crossVersion = true)
  )

  def handleCodeGeneratorRequest(request: CodeGeneratorRequest): CodeGeneratorResponse = {
    val b = CodeGeneratorResponse.newBuilder
    parseParameters(request.getParameter) match {
      case Right(params) =>
        try {
          val filesByName: Map[String, FileDescriptor] =
            request.getProtoFileList.asScala.foldLeft[Map[String, FileDescriptor]](Map.empty) {
              case (acc, fp) =>
                val deps = fp.getDependencyList.asScala.map(acc)
                acc + (fp.getName -> FileDescriptor.buildFrom(fp, deps.toArray))
            }
          val descriptorImplicits = new DescriptorImplicits(params, filesByName.values.toSeq)
          val validator = new ProtoValidation(descriptorImplicits)
          filesByName.values.foreach(fd => validator.validateFile(fd))
          request.getFileToGenerateList.asScala.foreach { name =>
            val file = filesByName(name)
            val responseFiles = generateServiceFiles(file)(descriptorImplicits)
            b.addAllFile(responseFiles.asJava)
          }
        } catch {
          case e: GeneratorException =>
            b.setError(e.message)
        }
      case Left(error) =>
        b.setError(error)
    }
    b.build
  }

  private def generateServiceFiles(file: FileDescriptor)(
      implicit descriptorImplicits: DescriptorImplicits): Seq[CodeGeneratorResponse.File] = {
    file.getServices.asScala.map { service =>
      import descriptorImplicits._
      val code = new LoggingGrpcServicePrinter(service).printService(FunctionalPrinter()).result()
      val b = CodeGeneratorResponse.File.newBuilder()
      b.setName(file.scalaDirectory + "/" + service.objectName + "Logging.scala")
      b.setContent(code)
      b.build
    }
  }
}

final class LoggingGrpcServicePrinter(service: ServiceDescriptor)(
    implicit descriptorImplicits: DescriptorImplicits) {
  import descriptorImplicits._
  private[this] def observer(typeParam: String): String = s"$streamObserver[$typeParam]"

  private val loggingPackage = "com.digitalasset.ledger.api.logging"
  private val loggingServiceMarker = loggingPackage + ".LoggingServiceMarker"
  private val internalErrorLogging = loggingPackage + ".InternalErrorLogging"

  private[this] def serviceMethodSignature(method: MethodDescriptor): PrinterEndo = { p =>
    val q = p.add(s"abstract override def ${method.name}")
    method.streamType match {
      case StreamType.Unary =>
        q.add(
            s"(request: ${method.inputType.scalaType}): scala.concurrent.Future[${method.outputType.scalaType}] = {")
          .call(serviceImplementation(s"logFutureIfInternal(super.${method.name}(request))"))
          .add("}")
          .newline
      case StreamType.ClientStreaming =>
        q.add(s"(responseObserver: ${observer(method.outputType.scalaType)}): ${observer(
            method.inputType.scalaType)} = {")
          .call(
            serviceImplementation(s"super.${method.name}(loggingStreamObserver(responseObserver))"))
          .add("}")
          .newline
      case StreamType.ServerStreaming =>
        q.add(s"(request: ${method.inputType.scalaType}, responseObserver: ${observer(
            method.outputType.scalaType)}): Unit = {")
          .call(serviceImplementation(
            s"super.${method.name}(request, loggingStreamObserver(responseObserver))"))
          .add("}")
          .newline
      case StreamType.Bidirectional =>
        q.add(s"(responseObserver: ${observer(method.outputType.scalaType)}): ${observer(
            method.inputType.scalaType)} = {")
          .call(
            serviceImplementation(s"super.${method.name}(loggingStreamObserver(responseObserver))"))
          .add("}")
          .newline
    }
  }

  private def serviceImplementation(implementation: String): PrinterEndo = { p =>
    p.indent.call(catchSyncErrors(implementation)).outdent
  }

  private[this] def catchSyncErrors(operation: String): PrinterEndo = { p =>
    p.add("try {")
      .addIndented(operation)
      .add("} catch {")
      .indent
      .add("case NonFatal(t) => ")
      .addIndented("logIfInternal(t)")
      .addIndented("throw t")
      .outdent
      .add("}")
  }

  private def addLogging(str: String): String =
    if (str.endsWith("`")) str.stripSuffix("`") + "Logging`" else str + "Logging"

  private[this] def serviceTrait: PrinterEndo = {
    val endos: PrinterEndo = { p =>
      p.call(service.methods.map(m => serviceMethodSignature(m)): _*)
    }

    p =>
      val realServiceTrait = s"${service.objectName}.${service.name}"
      val loggerName = new StringBuilder()
        .append('"')
        .append(service.getFile.scalaPackageName)
        .append('.')
        .append(service.getName)
        .append('"')
      p.add(
          s"trait ${addLogging(service.name)} extends $realServiceTrait with $loggingServiceMarker with  $internalErrorLogging {")
        .indent
        .add(s"protected def logger: org.slf4j.Logger")
        .newline
        .call(endos)
        .outdent
        .add("}")
  }

  private[this] val streamObserver = "_root_.io.grpc.stub.StreamObserver"

  def printService(printer: FunctionalPrinter): FunctionalPrinter = {
    printer
      .add("package " + service.getFile.scalaPackageName, "", "import scala.util.control.NonFatal")
      .newline
      .call(serviceTrait)
      .newline
  }
}
