// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.banner

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
