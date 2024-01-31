// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.protoc.plugins.scalapb

import scala.reflect.io.Streamable

// This file is mostly copied over from ScalaPbCodeGenerator and ProtobufGenerator

object ScalaPbCompilerPlugin {
  def main(args: Array[String]): Unit = {
    val request = Streamable.bytes(System.in)
    System.out.write(scalapb.ScalaPbCodeGenerator.run(request))
  }
}
