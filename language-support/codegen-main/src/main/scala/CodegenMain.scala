// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.codegen

// java codegen
import com.digitalasset.daml.lf.codegen.{StandaloneMain => JavaMain}

// scala codegen
import com.digitalasset.codegen.{Main => ScalaMain}

object CodegenMain {

  def main(args: Array[String]): Unit = {
    args.toList match {
      case "java" :: rest => JavaMain.main(rest.toArray)
      case "scala" :: rest => ScalaMain.main(rest.toArray)
      case _ =>
        Console.err.println("Invalid input: expected one of [java,scala] as first parameter!")
        sys.exit(-1)
    }
  }
}
