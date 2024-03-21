// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import com.daml.lf.codegen.conf.Conf
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

object StandaloneMain extends StrictLogging {

  @deprecated("Use codegen font-end: com.daml.codegen.CodegenMain.main", "0.13.23")
  def main(args: Array[String]): Unit =
    try {
      Main.main(args)
    } catch {
      case NonFatal(t) =>
        logger.error(s"Error generating code: {}", t.getMessage)
        sys.exit(-1)
    }
}

object Main {
  @deprecated("Use codegen font-end: com.daml.codegen.CodegenMain.main", "0.13.23")
  def main(args: Array[String]): Unit =
    Conf.parse(args) match {
      case Some(conf) => CodeGenRunner.run(conf)
      case None =>
        throw new IllegalArgumentException(s"Invalid command line arguments: ${args.mkString(" ")}")
    }
}
