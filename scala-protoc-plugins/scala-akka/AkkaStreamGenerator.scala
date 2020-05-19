// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.protoc.plugins.akka

import com.google.protobuf.Descriptors._
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import protocbridge.ProtocCodeGenerator
import scalapb.compiler._
import scalapb.options.compiler.Scalapb

import scala.collection.JavaConverters._
import scala.reflect.io.Streamable

// This file is mostly copied over from ScalaPbCodeGenerator and ProtobufGenerator

object AkkaStreamCompilerPlugin {
  def main(args: Array[String]): Unit = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(Streamable.bytes(System.in), registry)
    System.out.write(AkkaStreamGenerator.handleCodeGeneratorRequest(request).toByteArray)
  }
}

class AkkaStreamGenerator(val params: GeneratorParams, files: Seq[FileDescriptor])
    extends DescriptorImplicits(params, files) {
  def generateServiceFiles(file: FileDescriptor): Seq[CodeGeneratorResponse.File] = {
    file.getServices.asScala.flatMap { service =>
      val p = new AkkaGrpcServicePrinter(service, params)
      p.printService(FunctionalPrinter()).fold[List[CodeGeneratorResponse.File]](Nil) { p =>
        val code = p.result()
        val b = CodeGeneratorResponse.File.newBuilder()
        b.setName(file.scalaDirectory + "/" + service.name + "AkkaGrpc.scala")
        b.setContent(code)
        List(b.build)
      }
    }
  }
}

object AkkaStreamGenerator extends ProtocCodeGenerator {
  override def run(req: Array[Byte]): Array[Byte] = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(req, registry)
    handleCodeGeneratorRequest(request).toByteArray
  }
  def parseParameters(params: String): Either[String, GeneratorParams] = {
    params
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .foldLeft[Either[String, GeneratorParams]](Right(GeneratorParams())) {
        case (Right(params), "java_conversions") => Right(params.copy(javaConversions = true))
        case (Right(params), "flat_package") => Right(params.copy(flatPackage = true))
        case (Right(params), "grpc") => Right(params.copy(grpc = true))
        case (Right(params), "single_line_to_proto_string") =>
          Right(params.copy(singleLineToProtoString = true))
        case (Right(params), "ascii_format_to_string") =>
          Right(params.copy(asciiFormatToString = true))
        case (Right(params), p) => Left(s"Unrecognized parameter: '$p'")
        case (x, _) => x
      }
  }

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
          val generator = new AkkaStreamGenerator(params, filesByName.values.toSeq)
          val validator = new ProtoValidation(generator)
          filesByName.values.foreach(validator.validateFile)
          request.getFileToGenerateList.asScala.foreach { name =>
            val file = filesByName(name)
            val responseFiles = generator.generateServiceFiles(file)
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

  val deprecatedAnnotation: String =
    """@scala.deprecated(message="Marked as deprecated in proto file", "")"""
}
