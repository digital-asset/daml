// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.codegen

import com.digitalasset.daml.lf.codegen.conf.Conf
import com.typesafe.scalalogging.StrictLogging

import scala.util.control.NonFatal

object StandaloneMain extends StrictLogging {

  @deprecated("Use codegen font-end: com.digitalasset.codegen.CodegenMain.main", "0.13.23")
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
  @deprecated("Use codegen font-end: com.digitalasset.codegen.CodegenMain.main", "0.13.23")
  def main(args: Array[String]): Unit =
    Conf.parse(args) match {
      case Some(conf) => CodeGenRunner.run(conf)
      case None =>
        throw new IllegalArgumentException(s"Invalid command line arguments: ${args.mkString(" ")}")
    }
}
