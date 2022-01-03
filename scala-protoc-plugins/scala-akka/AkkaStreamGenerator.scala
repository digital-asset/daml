// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.protoc.plugins.akka

import com.google.protobuf.Descriptors._
import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.{CodeGeneratorRequest, CodeGeneratorResponse}
import protocbridge.ProtocCodeGenerator
import protocgen.{CodeGenRequest, CodeGenResponse}
import scalapb.compiler._
import scalapb.options.Scalapb

import scala.jdk.CollectionConverters._

// This file is mostly copied over from ScalaPbCodeGenerator and ProtobufGenerator

object AkkaStreamGenerator extends ProtocCodeGenerator {
  override def run(req: Array[Byte]): Array[Byte] = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(req, registry)
    handleCodeGeneratorRequest(CodeGenRequest(request)).toCodeGeneratorResponse.toByteArray
  }

  def handleCodeGeneratorRequest(request: CodeGenRequest): CodeGenResponse =
    parseParameters(request.parameter) match {
      case Right(params) =>
        implicit val descriptorImplicits: DescriptorImplicits =
          DescriptorImplicits.fromCodeGenRequest(params, request)
        try {
          val filesByName: Map[String, FileDescriptor] =
            request.allProtos.map(fd => fd.getName -> fd).toMap
          val validator = new ProtoValidation(descriptorImplicits)
          filesByName.values.foreach(validator.validateFile)
          val responseFiles = request.filesToGenerate.flatMap(generateServiceFiles(_))
          CodeGenResponse.succeed(responseFiles)
        } catch {
          case exception: GeneratorException =>
            CodeGenResponse.fail(exception.message)
        }
      case Left(error) =>
        CodeGenResponse.fail(error)
    }

  private def parseParameters(params: String): Either[String, GeneratorParams] = {
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
        case (Right(_), p) => Left(s"Unrecognized parameter: '$p'")
        case (x, _) => x
      }
  }

  private def generateServiceFiles(
      file: FileDescriptor
  )(implicit
      descriptorImplicits: DescriptorImplicits
  ): collection.Seq[CodeGeneratorResponse.File] = {
    import descriptorImplicits._
    file.getServices.asScala.flatMap { service =>
      val printer = new AkkaGrpcServicePrinter(service)
      printer.printService(FunctionalPrinter()).fold[List[CodeGeneratorResponse.File]](Nil) { p =>
        val code = p.result()
        val fileBuilder = CodeGeneratorResponse.File.newBuilder()
        fileBuilder.setName(file.scalaDirectory + "/" + service.name + "AkkaGrpc.scala")
        fileBuilder.setContent(code)
        List(fileBuilder.build())
      }
    }
  }

  val deprecatedAnnotation: String =
    """@scala.deprecated(message="Marked as deprecated in proto file", "")"""
}
