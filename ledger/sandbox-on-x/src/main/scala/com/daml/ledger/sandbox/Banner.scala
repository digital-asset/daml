// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import java.io.PrintStream
import scala.io.Source

object Banner {

  private val classLoader = getClass.getClassLoader

  private val resourceName = "banner.txt"

  private def banner: String =
    Source
      .fromResource(resourceName, classLoader)
      .getLines()
      .mkString("\n")

  def show(out: PrintStream): Unit = out.println(banner)

}
