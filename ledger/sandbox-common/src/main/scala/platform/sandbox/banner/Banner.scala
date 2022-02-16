// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.banner

import java.io.PrintStream

import scala.io.Source

object Banner {
  private val classLoader = getClass.getClassLoader

  private val resourceName = "banner.txt"

  private def banner: String =
    if (classLoader.getResource(resourceName) != null)
      Source
        .fromResource(resourceName, classLoader)
        .getLines()
        .mkString("\n")
    else
      "Banner resource missing from classpath."

  def show(out: PrintStream): Unit = out.println(banner)
}
