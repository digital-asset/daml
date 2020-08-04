// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.banner

import java.io.PrintStream

import scala.io.Source

object Banner {
  def show(out: PrintStream): Unit = {
    val resourceName = "banner.txt"
    if (getClass.getClassLoader.getResource(resourceName) != null)
      out.println(
        Source
          .fromResource(resourceName)
          .getLines
          .mkString("\n"))
    else
      out.println("Banner resource missing from classpath.")
  }
}
