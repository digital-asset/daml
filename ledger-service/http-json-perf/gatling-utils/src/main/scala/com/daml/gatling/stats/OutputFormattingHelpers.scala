// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.gatling.stats

object OutputFormattingHelpers {
  val lineLength = 80
  val available = lineLength - 32

  val formatter = new java.text.DecimalFormat("###.###")

  def subtitle(title: String): String =
    ("---- " + title + " ").padTo(lineLength, '-')

  def printN[N](value: N)(implicit N: Numeric[N]): String =
    if (value == N.zero) "-" else formatter.format(value)
}
