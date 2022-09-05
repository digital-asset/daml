// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.protoc.plugins.akka

import com.google.protobuf.ExtensionRegistry
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import protocgen.CodeGenRequest
import scalapb.options.Scalapb

import scala.reflect.io.Streamable

object AkkaStreamCompilerPlugin {
  def main(args: Array[String]): Unit = {
    val registry = ExtensionRegistry.newInstance()
    Scalapb.registerAllExtensions(registry)
    val request = CodeGeneratorRequest.parseFrom(Streamable.bytes(System.in), registry)
    System.out.write(
      AkkaStreamGenerator
        .handleCodeGeneratorRequest(CodeGenRequest(request))
        .toCodeGeneratorResponse
        .toByteArray
    )
  }
}
