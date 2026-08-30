// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import com.digitalasset.daml.lf.codegen.JsCodeGenConf
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

private[codegen] object JsCodeGenMain extends StrictLogging {
  def main(args: Array[String]): Unit =
    try {
      JsCodeGenConf.parse(args, parserName = "codegen-js") match {
        case Some(conf) =>
          val damlVersion = sys.env.getOrElse("DAML_SDK_VERSION", "0.0.0")
          JsCodeGen.run(conf, damlVersion)
        case None =>
          throw new IllegalArgumentException(
            s"Invalid command line arguments: ${args.mkString(" ")}"
          )
      }
    } catch {
      case NonFatal(t) =>
        logger.error(s"Error generating js code: {}", t.getMessage)
        sys.exit(-1)
    }
}
